# --- VERSION: m_collector_v2.17_20260128_Human_Relay_Master ---

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
    },
    {
        "active": False,
        "name": "店舗3の名前", 
        "url": "URL",
        "sheet_id": "シートID"
    }
]

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
    if not text or text == "-" or text == " ": return 0
    normalized = text.replace('▲', '-').replace('－', '-').replace(',', '').strip()
    match = re.search(r'(-?\d+)', normalized)
    return int(match.group(1)) if match else 0

# ==========================================
# BLOCK: 3. 偵察ロジック（未取得リスト作成）
# ==========================================
async def get_filtered_tasks(page, store_url, store_name, existing_records):
    print(f"[{store_name}] 記事一覧をスキャンして未取得日を特定中...")
    
    # ページ内容を確定させるための「揺らし」
    await page.evaluate('window.scrollTo(0, 500)')
    await asyncio.sleep(1)
    await page.evaluate('window.scrollTo(0, 0)')

    # 全リンク取得
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
        
        # 既読判定： 「日付_店名」でチェック
        record_key = f"{norm_date}_{store_name}"
        if record_key in existing_records: continue

        current_dt = datetime.strptime(norm_date, "%Y/%m/%d")
        if start_dt <= current_dt <= end_dt:
            tasks.append({"url": item['href'], "date": norm_date})
    
    unique_tasks = list({v['url']:v for v in tasks}.values())
    return sorted(unique_tasks, key=lambda x: x['date'], reverse=True)

# ==========================================
# BLOCK: 4. 強奪ロジック（詳細データ抽出）
# ==========================================
async def scrape_day_data(page, url, expected_name):
    target_url = url + ("&" if "?" in url else "?") + "kishu=all"
    await page.goto(target_url, wait_until="load")
    await page.wait_for_timeout(5000)
    
    # ページタイトルから店名の整合性をチェック
    full_title = await page.title()
    if expected_name not in full_title:
        print(f"  [拒絶] 店名不一致を検知（{full_title}）。スキップ。")
        return None, None

    # 生店名の抽出
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
# BLOCK: 5. 司令部（ヒューマン・リレー実装版）
# ==========================================
async def main():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    gc = gspread.authorize(creds)

    print("\n--- プロジェクト・リレー起動 ---")
    print("1. 専用Chromeで『みんレポ』を開いてください。")
    print("2. 広告を消し、パズルがあれば解いて、店舗のリスト画面を表示させてください。")
    input(">> 準備ができたら、ここで Enter を押してください...")

    async with async_playwright() as p:
        print("\n専用Chromeに接続します...")
        try:
            browser = await p.chromium.connect_over_cdp("http://localhost:9222")
            context = browser.contexts[0]
            
            # アクティブな（中身のある）タブを特定
            page = None
            for p_obj in context.pages:
                if "min-repo.com" in p_obj.url:
                    page = p_obj
                    break
            
            if not page:
                print("【失敗】みんレポのタブが見つかりません。ブラウザで開いていますか？")
                return
            
            print(f"  [接続成功] ターゲット: {await page.title()}")
            await page.bring_to_front()

        except Exception as e:
            print(f"接続エラー: {e}"); return

        for store in TARGET_STORES:
            if not store["active"]: continue

            print(f"\n=== 攻略開始: {store['name']} ===")
            try:
                sheet = gc.open_by_key(store['sheet_id'])
                raw_sheet = sheet.worksheet("生データ")
                cal_sheet = sheet.worksheet("カレンダー")
                
                print("  既存データの整合性を確認中...")
                all_values = cal_sheet.get_all_values()
                existing_records = [f"{row[0]}_{row[1]}" for row in all_values]

                # 偵察
                tasks = await get_filtered_tasks(page, store['url'], store['name'], existing_records)
                print(f"  新たに取得が必要なレポートを {len(tasks)} 件発見。")

                for task in tasks:
                    day_data, real_store_name = await scrape_day_data(page, task['url'], store['name'])
                    if not day_data: continue

                    # 一括書き込み用データ作成
                    rows = [[task['date'], real_store_name, r['name'], r['num'], clean_number(r['diff']), clean_number(r['games'])] for r in day_data]
                    raw_sheet.append_rows(rows)
                    
                    t_diff = sum(r[4] for r in rows)
                    t_games = sum(r[5] for r in rows)
                    cal_sheet.append_row([task['date'], real_store_name, len(rows), t_diff, int(t_diff/len(rows)) if len(rows)>0 else 0, t_games])
                    
                    print(f"    -> {task['date']} 完了 / 差枚 {t_diff}")
                    await asyncio.sleep(random.uniform(2, 4))

                if store != [s for s in TARGET_STORES if s["active"]][-1]:
                    pause = random.uniform(30, 60)
                    print(f"\n店舗間休憩... {pause:.1f}秒")
                    await asyncio.sleep(pause)

            except Exception as e:
                print(f"エラー発生: {e}")

        print("\n全ての指令を完遂しました。お疲れ様でした！")

if __name__ == "__main__":
    asyncio.run(main())