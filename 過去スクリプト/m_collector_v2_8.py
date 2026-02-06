# --- VERSION: m_collector_v2.8_20260128_Resilient ---

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
        "sheet_id": "ここにスプレッドシートIDを"
    }
]
LIMIT_DATE = "2024-11-01" 

# ==========================================
# BLOCK: 2. 道具箱（汎用処理）
# ==========================================
def normalize_date(date_text):
    """日付を YYYY/MM/DD に統一"""
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
    """特殊記号を除去して数値化"""
    if not text or text == "-": return 0
    normalized = text.replace('▲', '-').replace('−', '-').replace('－', '-').replace('–', '-').replace('—', '-')
    normalized = normalized.replace(',', '').strip()
    match = re.search(r'(-?\d+)', normalized)
    return int(match.group(1)) if match else 0

# ==========================================
# BLOCK: 3. 偵察ロジック（記事一覧の取得）
# ==========================================
async def get_new_tasks(page, store_url, store_name, existing_dates):
    print(f"[{store_name}] 記事リストを読み込み中...")
    
    # すでにページにいるならリロードせず、いなければ移動
    if store_url not in page.url:
        await page.goto(store_url, wait_until="load")
    
    # 記事が出るまで最大10秒粘る（wait_for_selectorを強化）
    try:
        # articleタグか、h2タグ内のリンクが出るまで待つ
        await page.wait_for_selector('h2 a', timeout=10000)
    except:
        print("  [警告] 記事が自動で出ません。強制スクロールで呼び出します。")

    # 少し揺らして読み込みを促す
    await page.evaluate('window.scrollTo(0, 800)')
    await asyncio.sleep(2)
    await page.evaluate('window.scrollTo(0, 0)')

    # 画面上の「全てのリンク」を力ずくで取得
    links_data = await page.evaluate('''() => {
        return Array.from(document.querySelectorAll('a'))
            .map(a => ({
                title: a.innerText.trim(),
                href: a.href
            }))
            .filter(l => l.href.match(/\\/\\d+\\/$/)); // 数字だけで終わるレポートURLっぽいものに限定
    }''')

    print(f"  [現場報告] ページ内で候補となるリンクを {len(links_data)} 件発見。")

    valid_tasks = []
    for item in links_data:
        # 店名が含まれているか
        if store_name not in item['title']: continue
        
        # タイトルから日付(1/24等)を抽出
        date_match = re.search(r'(\d{4}/)?\d{1,2}/\d{1,2}', item['title'])
        if not date_match: continue
        
        norm_date = normalize_date(date_match.group(0))
        if not norm_date or norm_date in existing_dates: continue
        
        # 期間チェック
        if datetime.strptime(norm_date, "%Y/%m/%d") < datetime.strptime(LIMIT_DATE, "%Y-%m-%d"): continue
        
        valid_tasks.append({"url": item['href'], "date": norm_date})
    
    return valid_tasks

# ==========================================
# BLOCK: 4. 強奪ロジック（詳細データの抽出）
# ==========================================
async def scrape_day_data(page, url):
    target_url = url + ("&" if "?" in url else "?") + "kishu=all"
    print(f"  [巡回] {target_url}")
    await page.goto(target_url, wait_until="load")
    await page.wait_for_timeout(5000) # 数値反映待ち
    
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
# BLOCK: 5. 司令部（メイン実行）
# ==========================================
async def main():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    gc = gspread.authorize(creds)

    async with async_playwright() as p:
        print("専用Chromeに接続中...")
        try:
            browser = await p.chromium.connect_over_cdp("http://localhost:9222")
            context = browser.contexts[0]
            # アクティブなページ（みんレポ）を特定
            page = next((p_obj for p_obj in context.pages if "min-repo.com" in p_obj.url), None)
            if not page:
                print("みんレポのタブが見つかりません。"); return
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
                
                new_tasks = await get_new_tasks(page, store['url'], store['name'], existing_dates)
                print(f"--- 判定終了: 合計 {len(new_tasks)} 件の新規レポートを取得します ---")

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