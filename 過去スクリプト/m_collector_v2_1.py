import asyncio
from playwright.async_api import async_playwright
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import re
from datetime import datetime

# --- 司令塔：巡回対象リスト ---
TARGET_STORES = [
    {
        "name": "学園", 
        "url": "https://min-repo.com/tag/%e3%83%93%e3%83%83%e3%82%af%e3%83%9e%e3%83%bc%e3%83%81%e3%81%a4%e3%81%8f%e3%81%b0%e5%ad%a6%e5%9c%92%e3%81%ae%e6%a3%ae%e5%ba%97/",
        "sheet_id": "1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I"
    }
]
LIMIT_DATE = "2024-11-01" 
# ----------------------------

def normalize_date(date_text):
    clean_text = re.sub(r'\(.\)', '', date_text).strip()
    try:
        if clean_text.count('/') == 2:
            parts = clean_text.split('/')
            dt = datetime(int(parts[0]), int(parts[1]), int(parts[2]))
        else:
            parts = clean_text.split('/')
            dt = datetime(2026, int(parts[0]), int(parts[1]))
        return dt.strftime("%Y/%m/%d")
    except: return None

def clean_number(text):
    if not text or text == "-": return 0
    normalized = text.replace('▲', '-').replace('−', '-').replace('－', '-').replace('–', '-').replace('—', '-')
    normalized = normalized.replace(',', '').strip()
    match = re.search(r'(-?\d+)', normalized)
    return int(match.group(1)) if match else 0

async def get_new_tasks(page, store_url, store_name, existing_dates):
    current_url = page.url
    if store_url not in current_url:
        print(f"  [移動] ターゲットページへ移動中...")
        await page.goto(store_url, wait_until="load")
    else:
        print(f"  [継続] 既存のタブをそのまま解析します。")

    await page.evaluate('window.scrollTo(0, 800)')
    await page.wait_for_timeout(3000)

    items = await page.evaluate('''() => {
        return Array.from(document.querySelectorAll('a'))
            .map(a => ({ title: a.innerText.trim(), href: a.href }))
            .filter(l => l.href.includes('min-repo.com/'));
    }''')

    print(f"  [現場報告] ページ内で合計 {len(items)} 個のリンクを発見しました。")

    tasks = []
    limit_dt = datetime.strptime(LIMIT_DATE, "%Y-%m-%d")
    match_count = 0
    duplicate_count = 0

    for item in items:
        if store_name not in item['title']: continue
        match_count += 1
        date_match = re.search(r'(\d{4}/)?\d{1,2}/\d{1,2}', item['title'])
        if not date_match: continue
        norm_date = normalize_date(date_match.group(0))
        if not norm_date or norm_date in existing_dates:
            if norm_date in existing_dates: duplicate_count += 1
            continue
        if datetime.strptime(norm_date, "%Y/%m/%d") < limit_dt: continue
        tasks.append({"url": item['href'], "date": norm_date})
    
    print(f"  [内訳] 店名一致: {match_count}件 / 既読スキップ: {duplicate_count}件")
    return tasks

async def scrape_day_data(page, url):
    target_url = url + ("&" if "?" in url else "?") + "kishu=all"
    print(f"  [巡回] {target_url}")
    await page.goto(target_url, wait_until="load", timeout=60000)
    await page.wait_for_timeout(5000)
    return await page.evaluate('''() => {
        const results = [];
        document.querySelectorAll('table tr').forEach(row => {
            const cols = row.querySelectorAll('td');
            if (cols.length >= 5) {
                const name = cols[0].innerText.trim();
                if (name && !name.includes("機種")) {
                    results.push({
                        name: name, num: cols[1].innerText.trim(),
                        diff: cols[2].innerText.trim(), games: cols[3].innerText.trim()
                    });
                }
            }
        });
        return results;
    }''')

async def main():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    gc = gspread.authorize(creds)

    async with async_playwright() as p:
        print("専用Chromeに接続します...")
        try:
            browser = await p.chromium.connect_over_cdp("http://localhost:9222")
            context = browser.contexts[0]
            target_page = None
            for p_obj in context.pages:
                if "min-repo.com" in p_obj.url:
                    target_page = p_obj
                    break
            if not target_page:
                print("  [警告] みんレポが開かれているタブが見つかりませんでした。")
                return
            page = target_page
        except Exception as e:
            print(f"Chrome接続エラー: {e}"); return

        for store in TARGET_STORES:
            print(f"\n=== 攻略開始: {store['name']} ===")
            try:
                # --- スプレッドシート接続診断 ---
                try:
                    sheet = gc.open_by_key(store['sheet_id'])
                    raw_sheet = sheet.worksheet("生データ")
                    cal_sheet = sheet.worksheet("カレンダー")
                    print("  [成功] スプレッドシートへの接続を確認しました。")
                except gspread.exceptions.SpreadsheetNotFound:
                    print(f"  [致命的エラー] IDが間違っているか、共有設定がされていません。")
                    continue
                except gspread.exceptions.WorksheetNotFound:
                    print(f"  [致命的エラー] タブ名が『生データ』『カレンダー』ではありません。")
                    continue
                except Exception as e:
                    print(f"  [エラー] スプレッドシート接続中に不明な問題: {e}")
                    continue

                print("スプレッドシートの履歴を確認中...")
                existing_dates = cal_sheet.col_values(1)
                
                new_tasks = await get_new_tasks(page, store['url'], store['name'], existing_dates)
                print(f"未取得のレポートを {len(new_tasks)} 件発見しました。")

                for task in sorted(new_tasks, key=lambda x: x['date'], reverse=True):
                    day_data = await scrape_day_data(page, task['url'])
                    if not day_data: continue
                    rows_to_add = [[task['date'], item['name'], item['num'], clean_number(item['diff']), clean_number(item['games'])] for item in day_data]
                    if rows_to_add:
                        raw_sheet.append_rows(rows_to_add)
                        t_diff = sum(r[3] for r in rows_to_add)
                        cal_sheet.append_row([task['date'], len(day_data), t_diff, int(t_diff/len(day_data)), sum(r[4] for r in rows_to_add)])
                        print(f"  [成功] {task['date']} 書き込み完了")
                    await asyncio.sleep(2)
            except Exception as e:
                print(f"  [中断] {store['name']}: {e}")

if __name__ == "__main__":
    asyncio.run(main())