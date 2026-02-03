# --- VERSION: m_collector_v4.2_Guardrail_Edition_20260201 ---

import asyncio
import random
import csv
import os
from playwright.async_api import async_playwright
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import re
from datetime import datetime, timedelta

# ==========================================
# BLOCK: 1. 固定設定エリア
# ==========================================
SPREADSHEET_KEY = "1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I"
MASTER_SHEET    = "店舗管理マスタ"
CALENDAR_SHEET  = "カレンダー"
LOCAL_DATABASE  = "/Users/macuser/Desktop/minrepo_project/minrepo_database.csv"
NORMAL_INTERVAL = 14400 # 4時間

# ==========================================
# BLOCK: 2. 道具箱（修正版）
# ==========================================
def hex_to_rgb(hex_str):
    hex_str = hex_str.lstrip('#')
    return {"red": int(hex_str[0:2], 16)/255.0, "green": int(hex_str[2:4], 16)/255.0, "blue": int(hex_str[4:6], 16)/255.0}

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
    if not text or text in ["-", " ", "±0"]: return 0
    normalized = str(text).replace('▲', '-').replace('－', '-').replace(',', '').strip()
    match = re.search(r'(-?\d+)', normalized)
    return int(match.group(1)) if match else 0

def get_local_existing_records(target_stores):
    records = set()
    if os.path.exists(LOCAL_DATABASE):
        with open(LOCAL_DATABASE, mode='r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 2:
                    for store in target_stores:
                        if store['name'] in row[1]:
                            records.add(f"{row[0]}_{store['name']}")
    return records

def save_to_local_csv(rows):
    with open(LOCAL_DATABASE, mode='a', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(rows)

# ==========================================
# BLOCK: 3. 偵察・強奪ロジック（ガードレール強化版）
# ==========================================
async def get_tasks(page, store_url, store_name, status, start_date_str, end_date_str, existing_records):
    print(f"  [{store_name}] 記事一覧をスキャン中...")
    await page.goto(store_url, wait_until="load")
    
    is_patrol = "巡回モード" in status
    
    # 巡回なら浅く、収集なら深く掘る
    scan_limit = 5 if is_patrol else 25
    last_count = 0
    for _ in range(scan_limit):
        await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
        await asyncio.sleep(1.5)
        current_links = await page.evaluate('document.querySelectorAll("table tr td a").length')
        if current_links == last_count: break
        last_count = current_links

    links_data = await page.evaluate('''() => {
        return Array.from(document.querySelectorAll('table tr td a'))
            .map(a => ({ title: a.innerText.trim(), href: a.href }))
            .filter(l => l.href.match(/\\/\\d+\\/$/));
    }''')

    tasks = []
    # 【最重要：指示の同期】開始日・終了日のパース
    try:
        s_dt = datetime.strptime(start_date_str, "%Y/%m/%d")
    except:
        s_dt = datetime(2024, 1, 1) # 指定なしなら2024年から

    if is_patrol:
        e_dt = datetime(2030, 12, 31) # 巡回モードなら常に最新を追う
    else:
        try:
            e_dt = datetime.strptime(end_date_str, "%Y/%m/%d")
        except:
            e_dt = datetime.now() # 指定なしなら今日まで

    for item in links_data:
        date_match = re.search(r'(\d{4}/)?\d{1,2}/\d{1,2}', item['title'])
        if not date_match: continue
        norm_date = normalize_date(date_match.group(0))
        if not norm_date or f"{norm_date}_{store_name}" in existing_records: continue
        
        current_dt = datetime.strptime(norm_date, "%Y/%m/%d")
        # 【ガードレール】期間外は徹底排除
        if s_dt <= current_dt <= e_dt:
            tasks.append({"url": item['href'], "date": norm_date})
    
    return sorted(list({v['url']:v for v in tasks}.values()), key=lambda x: x['date'], reverse=True)

# ==========================================
# BLOCK: 4. メインエンジン
# ==========================================
async def run_cycle():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    gc = gspread.authorize(creds)
    doc = gc.open_by_key(SPREADSHEET_KEY)
    master_ws = doc.worksheet(MASTER_SHEET)
    cal_ws = doc.worksheet(CALENDAR_SHEET)

    master_data = master_ws.get_all_values()[1:]
    yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y/%m/%d")
    
    async with async_playwright() as p:
        try:
            browser = await p.chromium.connect_over_cdp("http://localhost:9222")
            page = browser.contexts[0].pages[0]
            
            for idx, row in enumerate(master_data, start=2):
                if len(row) < 3: continue
                name, url, status, s_date, e_date = row[0], row[1], row[2], row[3], row[4]
                if status == "停止" or not url: continue
                
                # 更新済みスキップ（巡回用）
                existing = get_local_existing_records([{"name": name}])
                if status == "巡回モード" and f"{yesterday_str}_{name}" in existing:
                    print(f"  [完了済] {name} は更新済みです。")
                    continue

                print(f"\n=== {name} 攻略開始 ({status}) ===")
                master_ws.update_acell(f'C{idx}', status.replace("未着手", "作業中"))
                
                # 指示された期間(s_date, e_date)を渡す
                tasks = await get_tasks(page, url, name, status, s_date, e_date, existing)
                
                if not tasks:
                    print(f"    [待機] 新着なし。")
                    master_ws.update_acell(f'C{idx}', "巡回モード" if "巡回" in status else status.replace("作業中", "未着手"))
                    continue

                # 取得した中で最も新しい日付を記録する準備
                latest_date_in_this_run = tasks[0]['date']

                for task in tasks:
                    target_url = task['url'] + ("&" if "?" in task['url'] else "?") + "kishu=all"
                    await page.goto(target_url, wait_until="load")
                    await page.wait_for_timeout(5000)
                    
                    data = await page.evaluate('''() => {
                        const res = [];
                        document.querySelectorAll('table tr').forEach(row => {
                            const cols = row.querySelectorAll('td');
                            if (cols.length >= 3) {
                                const n = cols[0].innerText.trim();
                                if (n && !n.includes("機種") && !n.includes("平均")) {
                                    res.push({ name: n, num: cols[1].innerText, diff: cols[2].innerText, games: cols[3].innerText });
                                }
                            }
                        });
                        return res;
                    }''')
                    
                    if data:
                        rows = [[task['date'], name, r['name'], r['num'], clean_number(r['diff']), clean_number(r['games'])] for r in data]
                        save_to_local_csv(rows)
                        t_diff, t_games = sum(r[4] for r in rows), sum(r[5] for r in rows)
                        cal_ws.append_row([task['date'], name, len(rows), t_diff, int(t_diff/len(rows)) if len(rows)>0 else 0, t_games])
                        print(f"      -> {task['date']} 格納完了")

                # 【賢いスタンプ】全タスク終了後、最新の日付を1回だけ終了日に書く
                master_ws.update_acell(f'E{idx}', latest_date_in_this_run)
                master_ws.update_acell(f'F{idx}', datetime.now().strftime('%m/%d %H:%M'))
                master_ws.update_acell(f'C{idx}', "巡回モード")
                master_ws.format(f'C{idx}', {"backgroundColor": hex_to_rgb("#d9ead3")})

        except Exception as e:
            print(f"システムエラー: {e}")

# ==========================================
# BLOCK: 5. 司令部（時限式ループ）
# ==========================================
async def main():
    print("\n--- Ver.4.2 M1基地・Guardrailエディション 起動 ---")
    while True:
        now = datetime.now()
        current_time = now.strftime("%H:%M")
        
        if ("08:30" <= current_time <= "08:50") or ("09:30" <= current_time <= "09:50"):
            wait_sec = random.randint(60, 150)
            mode_label = "【超・緊急】ラッシュモード"
        elif "08:00" <= current_time <= "10:00":
            wait_sec = 600
            mode_label = "【朝】監視モード"
        else:
            wait_sec = NORMAL_INTERVAL
            mode_label = "【通常】省エネモード"

        print(f"\n[{now.strftime('%H:%M:%S')}] {mode_label} 巡回開始。")
        await run_cycle()
        print(f"次回巡回予定: {(datetime.now() + timedelta(seconds=wait_sec)).strftime('%H:%M:%S')}")
        await asyncio.sleep(wait_sec)

if __name__ == "__main__":
    asyncio.run(main())