import asyncio
from playwright.async_api import async_playwright
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import re
from datetime import datetime

# --- 司令塔：最もシンプルだった時の設定へ ---
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

async def main():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    gc = gspread.authorize(creds)

    async with async_playwright() as p:
        print("専用Chromeに接続中...")
        try:
            browser = await p.chromium.connect_over_cdp("http://localhost:9222")
            context = browser.contexts[0]
            
            # 【重要】クッキーは絶対に消さない（リーダーの人間証明を維持）
            
            # 全てのタブの中から、最も「リンクが多い」タブを自動選択
            best_page = None
            max_links = -1
            print(f"全 {len(context.pages)} 個のタブから最適なページを選定中...")
            
            for p_obj in context.pages:
                try:
                    count = await p_obj.evaluate('document.querySelectorAll("a").length')
                    if count > max_links:
                        max_links = count
                        best_page = p_obj
                except: continue
            
            if not best_page or max_links == 0:
                print("有効なページが見つかりません。みんレポを開き直してください。"); return
            
            page = best_page
            print(f"  [確定] 接続先タブ: '{await page.title()}' (リンク数: {max_links})")

        except Exception as e:
            print(f"接続エラー: {e}"); return

        for store in TARGET_STORES:
            print(f"\n=== {store['name']} 攻略開始 ===")
            try:
                sheet = gc.open_by_key(store['sheet_id'])
                raw_sheet = sheet.worksheet("生データ")
                cal_sheet = sheet.worksheet("カレンダー")
                existing_dates = cal_sheet.col_values(1)
                
                # リンク取得（成功時のシンプル・ロジック）
                print("リンクをスキャン中...")
                links_data = await page.evaluate('''() => {
                    return Array.from(document.querySelectorAll('a'))
                        .map(a => ({ title: a.innerText.trim(), href: a.href }));
                }''')

                # --- get_new_tasks 部分の診断強化版 ---

                valid_tasks = []
                print(f"--- 診断中: 画面から抽出したタイトル ---")
                
                # とにかく画面上のリンクの先頭5つを生で表示
                for i, item in enumerate(links_data[:5]):
                    print(f"  [{i}] タイトル: '{item['title']}'")

                for item in links_data:
                    if store['name'] not in item['title']: continue
                    date_match = re.search(r'(\d{4}/)?\d{1,2}/\d{1,2}', item['title'])
                    if not date_match: continue
                    
                    norm_date = normalize_date(date_match.group(0))
                    if not norm_date: continue

                    # ここで「既読チェック」の結果をカウント
                    if norm_date in existing_dates:
                        continue # 既読ならスキップ
                    
                    if datetime.strptime(norm_date, "%Y/%m/%d") < limit_dt: continue
                    valid_tasks.append({"url": item['href'], "date": norm_date})

                print(f"--- 診断終了: 新規タスクは {len(valid_tasks)}件 ---")

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
                                results.push({ name: cols[0].innerText, num: cols[1].innerText, diff: cols[2].innerText, games: cols[3].innerText });
                            }
                        });
                        return results;
                    }''')
                    
                    if day_data:
                        rows = [[task['date'], r['name'], r['num'], clean_number(r['diff']), clean_number(r['games'])] for r in day_data if "機種" not in r['name']]
                        raw_sheet.append_rows(rows)
                        t_diff = sum(r[3] for r in rows)
                        cal_sheet.append_row([task['date'], len(rows), t_diff, int(t_diff/len(rows)), sum(r[4] for r in rows)])
                        print(f"    -> 書き込み完了")
                    
                    await asyncio.sleep(2)

            except Exception as e:
                print(f"エラー: {e}")

if __name__ == "__main__":
    asyncio.run(main())