import asyncio
from playwright.async_api import async_playwright
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import re

# --- 司令塔：巡回対象リスト ---
TARGET_STORES = [
    {
        "name": "学園", # 判定用のキーワード（緩めでOK）
        "url": "https://min-repo.com/tag/%e3%83%93%e3%83%83%e3%82%af%e3%83%9e%e3%83%bc%e3%83%81%e3%81%a4%e3%81%8f%e3%81%b0%e5%ad%a6%e5%9c%92%e3%81%ae%e6%a3%ae%e5%ba%97/",
        "sheet_id": "1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I"
    }
]
# ----------------------------

def clean_number(text):
    if not text or text == "-": return 0
    normalized = text.replace('▲', '-').replace('−', '-').replace('－', '-').replace('–', '-').replace('—', '-')
    normalized = normalized.replace(',', '').strip()
    match = re.search(r'(-?\d+)', normalized)
    return int(match.group(1)) if match else 0

async def get_valid_links(page, store_url, store_name):
    print(f"[{store_name}] のレポートリストを取得中...")
    await page.goto(store_url, wait_until="load")
    
    # 記事リストが出現するまで「執拗に」スクロールして待つ
    print("  記事リストを召喚中...")
    for _ in range(3):
        await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
        await page.wait_for_timeout(1000)

    # 【底引き網】店名を見ず、URLの形（数字のみ）だけでリンクをすべて拾う
    links = await page.evaluate('''() => {
        return Array.from(document.querySelectorAll('a'))
            .map(a => a.href)
            .filter(href => href.match(/min-repo\\.com\/\\d+\\/$/)); // 数字で終わるURLを抽出
    }''')
    
    valid_links = list(set(links))
    print(f"  [発見] 数字形式のレポート候補を {len(valid_links)} 件見つけました。")
    return valid_links

async def scrape_day_with_retry(page, url, store_name):
    """1日分のデータを強奪（店名チェックはここで行う）"""
    await page.goto(url + "?kishu=all", wait_until="load")
    await page.wait_for_timeout(5000) # 数値反映をじっくり待つ
    
    title = await page.title()
    # ここで初めて「本当に学園の森か？」を判定
    if store_name not in title:
        print(f"  [スキップ] 他店データを検知: {title}")
        return None, None

    data = await page.evaluate('''() => {
        const results = [];
        const rows = document.querySelectorAll('table tr');
        rows.forEach(row => {
            const cols = row.querySelectorAll('td');
            if (cols.length >= 5) {
                const name = cols[0].innerText.trim();
                if (name && !name.includes("機種")) {
                    results.push({ 
                        name: name, 
                        num: cols[1].innerText.trim(), 
                        diff: cols[2].innerText.trim(), 
                        games: cols[3].innerText.trim() 
                    });
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
        try:
            browser = await p.chromium.connect_over_cdp("http://localhost:9222")
            context = browser.contexts[0]
            # アクティブなタブ、あるいは新しいタブを取得
            page = context.pages[0] if context.pages else await context.new_page()
        except Exception as e:
            print(f"Chrome接続エラー: {e}")
            return

        for store in TARGET_STORES:
            print(f"\n=== 攻略開始: {store['name']} ===")
            try:
                sheet = gc.open_by_key(store['sheet_id'])
                raw_sheet = sheet.worksheet("生データ")
                cal_sheet = sheet.worksheet("カレンダー")
                existing_dates = cal_sheet.col_values(1)
                
                links = await get_valid_links(page, store['url'], store['name'])

                # 新しいレポート（数字が大きい順）にソートして実行
                for link in sorted(links, reverse=True):
                    data, date_label = await scrape_day_with_retry(page, link, store['name'])
                    
                    if not data: continue
                    if date_label in existing_dates:
                        print(f"  [既読] {date_label} スキップ。")
                        continue

                    # データの集計と書き込み
                    rows_to_add = []
                    total_diff, total_games = 0, 0
                    for item in data:
                        diff, games = clean_number(item['diff']), clean_number(item['games'])
                        rows_to_add.append([date_label, item['name'], item['num'], diff, games])
                        total_diff += diff
                        total_games += games
                    
                    if rows_to_add:
                        raw_sheet.append_rows(rows_to_add)
                        avg_diff = int(total_diff / len(data)) if len(data) > 0 else 0
                        cal_sheet.append_row([date_label, len(data), total_diff, avg_diff, total_games])
                        print(f"  [成功] {date_label} 書き込み完了（差枚: {total_diff}）")
                    
                    await asyncio.sleep(2)

            except Exception as e:
                print(f"  [重大エラー] {store['name']} 攻略中にトラブル: {e}")

if __name__ == "__main__":
    asyncio.run(main())