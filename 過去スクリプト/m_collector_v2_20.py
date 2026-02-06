# --- VERSION: m_collector_v2.20_20260128_Absolute_Tab_Sync ---

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
START_DATE = "2025-09-01" 
END_DATE   = "2026-01-27" 

TARGET_STORES = [
    {
        "active": True,
        "name": "学園", 
        "url": "https://min-repo.com/tag/%e3%83%93%e3%83%83%e3%82%af%e3%83%9e%e3%83%bc%e3%83%81%e3%81%a4%e3%81%8f%e3%81%b0%e5%ad%a6%e5%9c%92%e3%81%ae%e6%a3%ae%e5%ba%97/",
        "sheet_id": "1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I"
    },
    {
        "active": True,
        "name": "マルハンつくば", 
        "url": "https://min-repo.com/tag/%e3%83%9e%e3%83%ab%e3%83%8f%e3%83%b3%e3%81%a4%e3%81%8f%e3%81%b0%e5%ad%a6%e5%9c%92%e3%81%ae%e6%a3%ae%e5%ba%97/", # ここは適宜正しいURLへ
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
# BLOCK: 3. 偵察ロジック
# ==========================================
async def get_filtered_tasks(page, store_url, store_name, existing_records):
    print(f"[{store_name}] 記事一覧をスキャン中...")
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
        
        record_key = f"{norm_date}_{store_name}"
        if record_key in existing_records: continue

        current_dt = datetime.strptime(norm_date, "%Y/%m/%d")
        if start_dt <= current_dt <= end_dt:
            tasks.append({"url": item['href'], "date": norm_date})
    
    unique_tasks = list({v['url']:v for v in tasks}.values())
    return sorted(unique_tasks, key=lambda x: x['date'], reverse=True)

# ==========================================
# BLOCK: 4. 強奪ロジック
# ==========================================
async def scrape_day_data(page, url, expected_name):
    target_url = url + ("&" if "?" in url else "?") + "kishu=all"
    await page.goto(target_url, wait_until="load")
    await page.wait_for_timeout(5000)
    
    full_title = await page.title()
    if expected_name not in full_title:
        print(f"  [拒絶] 店名不一致（{full_title}）。スキップ。")
        return None, None

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
                        name: name, num: cols[1].innerText.trim(),
                        diff: cols[2].innerText.trim(), games: cols[3].innerText.trim()
                    });
                }
            }
        });
        return results;
    }''')
    return data, extracted_store_name

# ==========================================
# BLOCK: 5. 司令部（絶対同期版）
# ==========================================
async def main():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    gc = gspread.authorize(creds)

    print("\n--- プロジェクト・リレー Ver.2.20 ---")
    input(">> 専用Chromeが起動していれば、ここで Enter を押してください...")

    async with async_playwright() as p:
        print("\n専用Chromeに接続します...")
        try:
            browser = await p.chromium.connect_over_cdp("http://localhost:9222")
            context = browser.contexts[0]
            
            page = None
            print(f"  開いているタブのURLを徹底点呼中...")
            
            for p_obj in context.pages:
                u = p_obj.url
                print(f"    - 見つかったURL: {u}")
                if "min-repo.com" in u:
                    page = p_obj
                    break
            
            if not page:
                print("\n  [！] みんレポのタブが見つからないため、新しく開きます。")
                page = await context.new_page()
                await page.goto("https://min-repo.com/")
                print("  [指示] ブラウザで『みんレポ』が開きました。店を選択し、数値が見える状態にしてください。")
                input("  >> 準備ができたら、このターミナルで Enter を押してください...")
            
            print(f"\n  [接続成功★] ターゲット確定: {page.url[:60]}")
            await page.bring_to_front()

        except Exception as e:
            print(f"接続エラー: {e}"); return

        for store in TARGET_STORES:
            if not store["active"]: continue
            print(f"\n=== {store['name']} 攻略開始 ===")
            try:
                sheet = gc.open_by_key(store['sheet_id'])
                raw_sheet = sheet.worksheet("生データ")
                cal_sheet = sheet.worksheet("カレンダー")
                print("  既存データの整合性を確認中...")
                all_values = cal_sheet.get_all_values()
                existing_records = [f"{row[0]}_{row[1]}" for row in all_values if len(row) > 1]

                tasks = await get_filtered_tasks(page, store['url'], store['name'], existing_records)
                print(f"  新たに取得が必要なレポートを {len(tasks)} 件発見。")

                for task in tasks:
                    day_data, real_store_name = await scrape_day_data(page, task['url'], store['name'])
                    if not day_data: continue

                    rows = [[task['date'], real_store_name, r['name'], r['num'], clean_number(r['diff']), clean_number(r['games'])] for r in day_data]
                    raw_sheet.append_rows(rows)
                    t_diff = sum(r[4] for r in rows)
                    cal_sheet.append_row([task['date'], real_store_name, len(rows), t_diff, int(t_diff/len(rows)) if len(rows)>0 else 0, sum(r[5] for r in rows)])
                    print(f"    -> {task['date']} [{real_store_name}] 完了 / 差枚 {t_diff}")
                    await asyncio.sleep(random.uniform(2, 4))

                if store != [s for s in TARGET_STORES if s["active"]][-1]:
                    pause = random.uniform(20, 30)
                    print(f"\n店舗間休憩... {pause:.1f}秒")
                    await asyncio.sleep(pause)

            except Exception as e:
                print(f"エラー発生: {e}")

        print("\n全ての指令を完遂しました。")

if __name__ == "__main__":
    asyncio.run(main())