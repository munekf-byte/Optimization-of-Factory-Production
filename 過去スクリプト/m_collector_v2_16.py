# --- VERSION: m_collector_v2.16_20260128_Integrity_Check ---

import asyncio
import random
from playwright.async_api import async_playwright
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import re
from datetime import datetime

# ==========================================
# BLOCK: 1. 司令塔（設定エリア）
# ==========================================
START_DATE = "2025-12-01" 
END_DATE   = "2026-01-27" 

TARGET_STORES = [
    {
        "active": True,
        "name": "学園",  # ページ判定用のキーワード
        "url": "https://min-repo.com/tag/%e3%83%93%e3%83%83%e3%82%af%e3%83%9e%e3%83%bc%e3%83%81%e3%81%a4%e3%81%8f%e3%81%b0%e5%ad%a6%e5%9c%92%e3%81%ae%e6%a3%ae%e5%ba%97/",
        "sheet_id": "1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I"
    },
    {
        "active": True,
        "name": "マルハンつくば", 
        "url": "https://min-repo.com/tag/%e3%83%9e%e3%83%ab%e3%83%8f%e3%83%b3%e3%81%a4%e3%81%8f%e3%81%b0%e5%ba%97/",
        "sheet_id": "1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I"
    }
]

# ==========================================
# BLOCK: 2. 道具箱
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
    if not text or text == "-" or text == " ": return 0
    normalized = text.replace('▲', '-').replace('－', '-').replace(',', '').strip()
    match = re.search(r'(-?\d+)', normalized)
    return int(match.group(1)) if match else 0

# ==========================================
# BLOCK: 3. 偵察ロジック（複合キー再開機能）
# ==========================================
async def get_filtered_tasks(page, store_url, store_name, existing_records):
    print(f"[{store_name}] 未取得の日付をスキャン中...")
    if store_url not in page.url:
        await page.goto(store_url, wait_until="load")
    
    await page.bring_to_front()
    await page.evaluate('window.scrollTo(0, 800)')
    await asyncio.sleep(1)
    await page.evaluate('window.scrollTo(0, 0)')

    links_data = await page.evaluate('''() => {
        return Array.from(document.querySelectorAll('table tr td a'))
            .map(a => ({ title: a.innerText.trim(), href: a.href }))
            .filter(l => l.href.match(/\\/\\d+\\/$/));
    }''')

    tasks = []
    start_dt = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_dt   = datetime.strptime(END_DATE, "%Y-%m-%d")

    for item in links_data:
        date_match = re.search(r'(\d{4}/)?\d{1,2}/\d{1,2}', item['title'])
        if not date_match: continue
        norm_date = normalize_date(date_match.group(0))
        if not norm_date: continue
        
        # 【重要】「日付_店舗名」の組み合わせで既読を判定
        # これにより、他店の同日データがあってもスキップされなくなります
        record_key = f"{norm_date}_{store_name}"
        if record_key in existing_records:
            continue

        current_dt = datetime.strptime(norm_date, "%Y/%m/%d")
        if start_dt <= current_dt <= end_dt:
            tasks.append({"url": item['href'], "date": norm_date})
    
    unique_tasks = list({v['url']:v for v in tasks}.values())
    return sorted(unique_tasks, key=lambda x: x['date'], reverse=True)

# ==========================================
# BLOCK: 4. 強奪ロジック（店舗名厳密チェック版）
# ==========================================
async def scrape_day_data(page, url, expected_name):
    target_url = url + ("&" if "?" in url else "?") + "kishu=all"
    await page.goto(target_url, wait_until="load")
    await page.wait_for_timeout(5000)
    
    # 【重要】ページタイトルから「真実の店名」を抜き出す
    full_title = await page.title()
    if expected_name not in full_title:
        print(f"  [拒絶] 店名不一致を検知（ターゲット: {expected_name} / 実際: {full_title}）。スキップします。")
        return None, None

    # 店名のエッセンスだけを抽出（例：1/24 マルハンつくば店 -> マルハンつくば店）
    extracted_store_name = full_title.split('|')[0].strip()
    extracted_store_name = re.sub(r'(\d{4}/)?\d{1,2}/\d{1,2}\(.\)', '', extracted_store_name).strip()

    data = await page.evaluate('''() => {
        const results = [];
        const rows = document.querySelectorAll('table tr');
        rows.forEach(row => {
            const cols = row.querySelectorAll('td');
            if (cols.length >= 3) { 
                const name = cols[0].innerText.trim();
                if (name && !name.includes("機種") && !name.includes("平均")) {
                    results.push({
                        name: name,
                        num: cols[1] ? cols[1].innerText.trim() : "0",
                        diff: cols[2] ? cols[2].innerText.trim() : "0",
                        games: cols[3] ? cols[3].innerText.trim() : "0"
                    });
                }
            }
        });
        return results;
    }''')
    return data, extracted_store_name

# ==========================================
# BLOCK: 5. 司令部
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
            page = next((p_obj for p_obj in context.pages if "min-repo.com" in p_obj.url), None)
            if not page:
                print("みんレポのタブが見つかりません。"); return
        except Exception as e:
            print(f"接続エラー: {e}"); return

        for store in TARGET_STORES:
            if not store["active"]: continue

            print(f"\n=== {store['name']} 攻略開始 ===")
            try:
                sheet = gc.open_by_key(store['sheet_id'])
                raw_sheet = sheet.worksheet("生データ")
                cal_sheet = sheet.worksheet("カレンダー")
                
                # 【重要】「日付 + 店名」のリストを作成して既読判定を正確にする
                print("  既存レコードの整合性を確認中...")
                all_values = cal_sheet.get_all_values()
                # A列(日付)とB列(店舗名)を合体させたキーのリストを作る
                existing_records = [f"{row[0]}_{row[1]}" for row in all_values]

                tasks = await get_filtered_tasks(page, store['url'], store['name'], existing_records)
                print(f"  新たに取得が必要なレポートを {len(tasks)} 件発見。")

                for task in tasks:
                    day_data, real_store_name = await scrape_day_data(page, task['url'], store['name'])
                    
                    if not day_data: continue

                    # 生データ作成（実況から取得した正確な店名を使用）
                    rows = [[task['date'], real_store_name, r['name'], r['num'], clean_number(r['diff']), clean_number(r['games'])] for r in day_data]
                    raw_sheet.append_rows(rows)
                    
                    t_diff = sum(r[4] for r in rows)
                    t_games = sum(r[5] for r in rows)
                    cal_sheet.append_row([task['date'], real_store_name, len(rows), t_diff, int(t_diff/len(rows)) if len(rows)>0 else 0, t_games])
                    
                    print(f"    -> {task['date']} [{real_store_name}] 完了 / 差枚 {t_diff}")
                    
                    wait_time = random.uniform(2, 5)
                    await asyncio.sleep(wait_time)

                if store != [s for s in TARGET_STORES if s["active"]][-1]:
                    store_break = random.uniform(20, 40)
                    print(f"\n店舗間休憩中... {store_break:.1f}秒")
                    await asyncio.sleep(store_break)

            except Exception as e:
                print(f"エラー発生: {e}")

        print("\n全ての攻略を完了しました。")

if __name__ == "__main__":
    asyncio.run(main())