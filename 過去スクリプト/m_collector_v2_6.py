import asyncio
from playwright.async_api import async_playwright
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import re
from datetime import datetime

# --- 設定エリア ---
TARGET_STORES = [
    {
        "name": "学園", 
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

async def main():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    gc = gspread.authorize(creds)

    async with async_playwright() as p:
        print("専用Chromeに接続中...")
        try:
            browser = await p.chromium.connect_over_cdp("http://localhost:9222")
            context = browser.contexts[0]
            
            # 全タブを表示して、リーダーの目線と一致させる
            print(f"現在開いているタブ一覧:")
            target_page = None
            for i, p_obj in enumerate(context.pages):
                title = await p_obj.title()
                url = p_obj.url
                print(f"  [{i}] タイトル: {title[:30]}... | URL: {url[:40]}...")
                if "min-repo.com" in url:
                    target_page = p_obj
            
            if not target_page:
                print("有効なページが見つかりません。"); return
            
            page = target_page
            print(f"\n[決定] 以下のタブを解析対象にします:\n  URL: {page.url}\n")

        except Exception as e:
            print(f"接続エラー: {e}"); return

        for store in TARGET_STORES:
            print(f"=== {store['name']} 攻略開始 ===")
            try:
                sheet = gc.open_by_key(store['sheet_id'])
                cal_sheet = sheet.worksheet("カレンダー")
                raw_sheet = sheet.worksheet("生データ")
                existing_dates = cal_sheet.col_values(1)
                
                print(f"画面からリンクをスキャン中...")
                links_data = await page.evaluate('''() => {
                    return Array.from(document.querySelectorAll('a'))
                        .map(a => ({ title: a.innerText.trim(), href: a.href }));
                }''')

                valid_tasks = []
                print(f"--- 抽出判定の実況中継 ---")
                
                # 最初に見つかった5つのリンクを無条件で表示
                for item in links_data[:5]:
                    print(f"  [発見] タイトル: '{item['title']}'")

                for item in links_data:
                    # 1. 店名チェック
                    if store['name'] not in item['title']:
                        continue
                    
                    # 2. 日付形式チェック
                    date_match = re.search(r'(\d{4}/)?\d{1,2}/\d{1,2}', item['title'])
                    if not date_match:
                        continue
                    
                    norm_date = normalize_date(date_match.group(0))
                    if not norm_date: continue

                    # 3. 重複チェック
                    if norm_date in existing_dates:
                        # 削除した日のテスト用：もし見つかったら報告
                        print(f"  [既読回避] {norm_date} はスプレッドシートにあるためスキップします。")
                        continue
                    
                    # 4. 期間チェック
                    if datetime.strptime(norm_date, "%Y/%m/%d") < datetime.strptime(LIMIT_DATE, "%Y-%m-%d"):
                        continue
                    
                    print(f"  [採用★] 未取得の日付を発見しました: {norm_date}")
                    valid_tasks.append({"url": item['href'], "date": norm_date})

                print(f"--- 判定終了: 合計 {len(valid_tasks)} 件の新しいレポートを取得します ---")

                # (取得・書き込みループは省略せずに継続)
                for task in sorted(valid_tasks, key=lambda x: x['date'], reverse=True):
                    print(f"  [巡回] {task['date']} を取得中...")
                    target_url = task['url'] + ("&" if "?" in task['url'] else "?") + "kishu=all"
                    await page.goto(target_url, wait_until="load")
                    await page.wait_for_timeout(5000)
                    
                    day_data = await page.evaluate('''() => {
                        const results = [];
                        document.querySelectorAll('table tr').forEach(row => {
                            const cols = row.querySelectorAll('td');
                            if (cols.length >= 5) {
                                const name = cols[0].innerText.trim();
                                if (name && !name.includes("機種")) {
                                    results.push({ name, num: cols[1].innerText, diff: cols[2].innerText, games: cols[3].innerText });
                                }
                            }
                        });
                        return results;
                    }''')
                    
                    if day_data:
                        rows = [[task['date'], r['name'], r['num'], clean_number(r['diff']), clean_number(r['games'])] for r in day_data]
                        raw_sheet.append_rows(rows)
                        t_diff = sum(r[3] for r in rows)
                        cal_sheet.append_row([task['date'], len(rows), t_diff, int(t_diff/len(rows)), sum(r[4] for r in rows)])
                        print(f"    -> {task['date']} 書き込み完了")
                    await asyncio.sleep(2)

            except Exception as e:
                print(f"エラー: {e}")

if __name__ == "__main__":
    asyncio.run(main())