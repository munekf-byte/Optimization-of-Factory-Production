# --- VERSION: m_collector_v2.14_20260128_Sync_Master ---

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
        "name": "学園の森", 
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
    if not text or text == "-" or text == " ": return 0
    normalized = text.replace('▲', '-').replace('－', '-').replace(',', '').strip()
    match = re.search(r'(-?\d+)', normalized)
    return int(match.group(1)) if match else 0

# ==========================================
# BLOCK: 3. 偵察ロジック（同期重視）
# ==========================================
async def get_filtered_tasks(page, store_url, store_name):
    print(f"[{store_name}] リストページへ移動中...")
    await page.goto(store_url, wait_until="load")
    await page.bring_to_front() # タブを最前面へ
    
    # 読み込みを確実にするためのスクロール
    await page.evaluate('window.scrollTo(0, 1000)')
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
        
        current_dt = datetime.strptime(norm_date, "%Y/%m/%d")
        if start_dt <= current_dt <= end_dt:
            tasks.append({"url": item['href'], "date": norm_date})
    
    unique_tasks = list({v['url']:v for v in tasks}.values())
    return sorted(unique_tasks, key=lambda x: x['date'], reverse=True)

# ==========================================
# BLOCK: 4. 強奪ロジック（同期重視）
# ==========================================
async def scrape_day_data(page, url):
    target_url = url + ("&" if "?" in url else "?") + "kishu=all"
    await page.goto(target_url, wait_until="load")
    # 画面が同期しているか確認するためのログ
    print(f"  [同期中] 現在のURL: {page.url[:60]}...")
    await page.wait_for_timeout(5000)
    
    return await page.evaluate('''() => {
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

# ==========================================
# BLOCK: 5. 司令部（タブ厳選・同期実行）
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
            
            # --- 【重要】最も中身が詰まっている「本物のタブ」を探す ---
            best_page = None
            max_content = -1
            for p_obj in context.pages:
                try:
                    content_length = len(await p_obj.content())
                    if "min-repo.com" in p_obj.url and content_length > max_content:
                        max_content = content_length
                        best_page = p_obj
                except: continue
            
            if not best_page:
                print("みんレポのタブが見つかりません。"); return
            page = best_page
            print(f"  [確定] ターゲットタブ: '{await page.title()}'")
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
                
                # 取得開始前にシートを一度リセット（案A: スクラップ＆ビルド）
                # ※最初の店舗の時だけリセットしたい場合はここを調整
                if store == [s for s in TARGET_STORES if s["active"]][0]:
                    print("  スプレッドシートをリセットしています...")
                    raw_sheet.clear()
                    raw_sheet.append_row(["日付", "店舗名", "機種名", "台番号", "差枚", "G数"])
                    cal_sheet.clear()
                    cal_sheet.append_row(["日付", "店舗名", "総台数", "総差枚", "平均差枚", "総G数"])

                tasks = await get_filtered_tasks(page, store['url'], store['name'])
                print(f"期間内の有効レポートを {len(tasks)} 件発見。")

                for task in tasks:
                    day_data = await scrape_day_data(page, task['url'])
                    if day_data:
                        rows = [[task['date'], store['name'], r['name'], r['num'], clean_number(r['diff']), clean_number(r['games'])] for r in day_data]
                        raw_sheet.append_rows(rows)
                        
                        t_diff = sum(r[4] for r in rows)
                        t_games = sum(r[5] for r in rows)
                        cal_sheet.append_row([task['date'], store['name'], len(rows), t_diff, int(t_diff/len(rows)) if len(rows)>0 else 0, t_games])
                        
                        print(f"    -> {task['date']} 完了 / 差枚 {t_diff}")
                    
                    wait_time = random.uniform(2, 5)
                    await asyncio.sleep(wait_time)

                # 店舗間休憩
                if store != [s for s in TARGET_STORES if s["active"]][-1]:
                    store_break = random.uniform(20, 40)
                    print(f"\n店舗間休憩中... {store_break:.1f}秒")
                    await asyncio.sleep(store_break)

            except Exception as e:
                print(f"エラー発生: {e}")

        print("\n全ての攻略を完了しました。")

if __name__ == "__main__":
    asyncio.run(main())