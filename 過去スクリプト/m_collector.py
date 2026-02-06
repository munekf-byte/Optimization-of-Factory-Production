import asyncio
from playwright.async_api import async_playwright
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import re
import time

# --- 司令塔：巡回対象リスト ---
TARGET_STORES = [
    {
        "name": "マルハンつくば店",
        "url": "https://min-repo.com/tag/%e3%83%9e%e3%83%ab%e3%83%8f%e3%83%b3%e3%81%a4%e3%81%8f%e3%81%b0%e5%ba%97/",
        "sheet_id": "リーダーのスプレッドシートID"
    },
    # 今後ここへ {"name": "店名", "url": "URL", "sheet_id": "ID"} を追加するだけで全自動化
]
# ----------------------------

def clean_number(text):
    if not text or text == "-": return 0
    normalized = text.replace('▲', '-').replace('−', '-').replace('－', '-').replace('–', '-').replace('—', '-')
    normalized = normalized.replace(',', '').strip()
    match = re.search(r'(-?\d+)', normalized)
    return int(match.group(1)) if match else 0

async def get_valid_links(page, store_url, store_name):
    """店舗トップからレポートリンクを抽出し、他店を除外する"""
    print(f"[{store_name}] のレポートリストを取得中...")
    await page.goto(store_url, wait_until="load")
    
    # ページ内の記事タイトルとリンクをセットで取得
    links_data = await page.evaluate('''() => {
        return Array.from(document.querySelectorAll('article')).map(article => {
            const a = article.querySelector('h2 a');
            return { title: a.innerText, href: a.href };
        });
    }''')
    
    # 店名が含まれているリンクのみに厳選
    valid_links = [l['href'] for l in links_data if store_name in l['title']]
    return list(set(valid_links))

async def scrape_day_with_retry(page, url, store_name):
    """1日分のデータを強奪（店名不一致なら即中止）"""
    await page.goto(url + "?kishu=all", wait_until="load")
    await page.wait_for_timeout(5000)
    
    title = await page.title()
    # ページタイトルに店名が含まれていない場合は「ゴミ」と判断
    if store_name not in title:
        print(f"  [警告] 店名不一致を検知（{title}）。スキップします。")
        return None, None

    data = await page.evaluate('''() => {
        const results = [];
        const rows = document.querySelectorAll('table tr');
        rows.forEach(row => {
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
    
    date_label = title.split('|')[0].strip()
    return data, date_label

async def main():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    gc = gspread.authorize(creds)

    async with async_playwright() as p:
        print("専用Chromeに接続します...")
        browser = await p.chromium.connect_over_cdp("http://localhost:9222")
        page = (browser.contexts[0]).pages[0]

        for store in TARGET_STORES:
            print(f"\n=== 攻略開始: {store['name']} ===")
            try:
                sheet = gc.open_by_key(store['sheet_id'])
                raw_sheet = sheet.worksheet("生データ")
                cal_sheet = sheet.worksheet("カレンダー")
                
                # すでに取得済みの「日付」リストを把握
                existing_dates = cal_sheet.col_values(1) # A列(日付)を全取得
                
                links = await get_valid_links(page, store['url'], store['name'])
                print(f"有効なレポートを {len(links)} 件発見。")

                for link in sorted(links, reverse=True):
                    # 日付判定（URLからIDを抜くなどの簡易判定も可能だが、一旦開いてから確認）
                    # 効率化のため、まずはページタイトル等の取得に挑戦（省略可）
                    
                    data, date_label = await scrape_day_with_retry(page, link, store['name'])
                    
                    if not data: continue
                    
                    if date_label in existing_dates:
                        print(f"  [既読] {date_label} は取得済みです。スキップ。")
                        continue

                    # ここで書き込み処理（前回のロジックと同じ）
                    rows_to_add = []
                    total_diff, total_games = 0, 0
                    for item in data:
                        diff, games = clean_number(item['diff']), clean_number(item['games'])
                        rows_to_add.append([date_label, item['name'], item['num'], diff, games])
                        total_diff += diff
                        total_games += games
                    
                    if rows_to_add:
                        raw_sheet.append_rows(rows_to_add)
                        avg_diff = int(total_diff / len(data))
                        cal_sheet.append_row([date_label, len(data), total_diff, avg_diff, total_games])
                        print(f"  [成功] {date_label} を書き込みました。")
                    
                    await asyncio.sleep(2)

            except Exception as e:
                print(f"  [重大エラー] {store['name']} の攻略中にトラブル発生: {e}")

if __name__ == "__main__":
    asyncio.run(main())