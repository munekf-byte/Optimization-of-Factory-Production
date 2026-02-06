# --- VERSION: m_collector_v2.12_20260128_Force_Sync_Edition ---

import asyncio
from playwright.async_api import async_playwright
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import re
from datetime import datetime

# ==========================================
# BLOCK: 1. 設定エリア
# ==========================================
TARGET_STORES = [
    {
        "name": "学園", 
        "url": "https://min-repo.com/tag/%e3%83%93%e3%83%83%e3%82%af%e3%83%9e%e3%83%bc%e3%83%81%e3%81%a4%e3%81%8f%e3%81%b0%e5%ad%a6%e5%9c%92%e3%81%ae%e6%a3%ae%e5%ba%97/",
        "sheet_id": "1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I"
    }
]
LIMIT_DATE = "2024-11-01" 

# ==========================================
# BLOCK: 2. 道具箱（汎用処理）
# ==========================================
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

# ==========================================
# BLOCK: 3. 偵察ロジック（強制同期スキャン）
# ==========================================
async def get_tasks_force_sync(page, store_name, existing_dates):
    print(f"ページの内容を強制同期しています...")
    
    # 【強制動作1】タブを最前面に呼び出す
    await page.bring_to_front()
    
    # 【強制動作2】タイトルが確定するまで最大5秒待機
    for _ in range(5):
        title = await page.title()
        if title: break
        print("  ...ページ情報の確定を待っています...")
        await page.evaluate('window.scrollBy(0, 10)') # 微振動
        await asyncio.sleep(1)

    # 視覚的確認：大きく揺らす
    await page.evaluate('window.scrollTo(0, 1000)')
    await asyncio.sleep(1)
    await page.evaluate('window.scrollTo(0, 0)')

    # 全リンクを一旦すべて取得して「見えるか」テスト
    all_links_count = await page.evaluate('document.querySelectorAll("a").length')
    print(f"  [診断] 現在ページ内に {all_links_count} 個のリンクを検知しています。")

    links_data = await page.evaluate('''() => {
        const results = [];
        const anchors = document.querySelectorAll('a');
        anchors.forEach(a => {
            // hrefに数字が含まれるレポートURLを抽出
            if (a.href && a.href.match(/\\/\\d+\\/$/)) {
                results.push({ title: a.innerText.trim(), href: a.href });
            }
        });
        return results;
    }''')

    valid_tasks = []
    limit_dt = datetime.strptime(LIMIT_DATE, "%Y-%m-%d")

    for item in links_data:
        date_match = re.search(r'(\d{4}/)?\d{1,2}/\d{1,2}', item['title'])
        if not date_match: continue
        norm_date = normalize_date(date_match.group(0))
        if not norm_date or norm_date in existing_dates: continue
        if datetime.strptime(norm_date, "%Y/%m/%d") < limit_dt: continue
        valid_tasks.append({"url": item['href'], "date": norm_date})
    
    return valid_tasks

# ==========================================
# BLOCK: 4. 強奪ロジック
# ==========================================
async def scrape_day_data(page, url):
    target_url = url + ("&" if "?" in url else "?") + "kishu=all"
    print(f"  [巡回] {target_url}")
    await page.goto(target_url, wait_until="load")
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

# ==========================================
# BLOCK: 5. 司令部（強制同期版）
# ==========================================
async def main():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    gc = gspread.authorize(creds)

    async with async_playwright() as p:
        print("専用Chromeに接続します...")
        try:
            browser = await p.chromium.connect_over_cdp("http://localhost:9222")
            context = browser.contexts[0]
            
            # URL優先でタブを特定
            page = next((p_obj for p_obj in context.pages if "min-repo.com" in p_obj.url and "tag" in p_obj.url), None)
            
            if not page:
                print("【エラー】店舗リスト（tag）ページが開かれているタブが見つかりません。")
                return

        except Exception as e:
            print(f"接続エラー: {e}"); return

        for store in TARGET_STORES:
            print(f"\n=== {store['name']} 攻略開始 ===")
            try:
                sheet = gc.open_by_key(store['sheet_id'])
                cal_sheet = sheet.worksheet("カレンダー")
                raw_sheet = sheet.worksheet("生データ")
                
                print("スプレッドシートの履歴を確認中...")
                existing_dates = cal_sheet.col_values(1)
                
                # 強制同期モードでタスク取得
                new_tasks = await get_tasks_force_sync(page, store['name'], existing_dates)
                print(f"--- 判定終了: 合計 {len(new_tasks)} 件の新規レポートを処理します ---")

                for task in sorted(new_tasks, key=lambda x: x['date'], reverse=True):
                    day_data = await scrape_day_data(page, task['url'])
                    if day_data:
                        rows = [[task['date'], r['name'], r['num'], clean_number(r['diff']), clean_number(r['games'])] for r in day_data]
                        raw_sheet.append_rows(rows)
                        t_diff = sum(r[3] for r in rows)
                        cal_sheet.append_row([task['date'], len(rows), t_diff, int(t_diff/len(rows)) if len(rows)>0 else 0, sum(r[4] for r in rows)])
                        print(f"    -> {task['date']} 書き込み完了")
                    await asyncio.sleep(2)

            except Exception as e:
                print(f"エラー: {e}")

if __name__ == "__main__":
    asyncio.run(main())