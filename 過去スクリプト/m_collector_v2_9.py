# --- VERSION: m_collector_v2.9_20260128_Visual_Table_Edition ---

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
        "sheet_id": "1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I"
    }
]
LIMIT_DATE = "2024-11-01" 

# ==========================================
# BLOCK: 2. 道具箱（汎用処理）
# ==========================================
def normalize_date(date_text):
    """日付を YYYY/MM/DD に統一。年号がない場合は2026年とみなす"""
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
# BLOCK: 3. 偵察ロジック（画像にあるテーブルからリンクを抜く）
# ==========================================
async def get_tasks_from_table(page, store_name, existing_dates):
    print(f"現在表示中の画面をスキャンしています...")
    
    # 【視覚的確認用】リーダーの画面をわざと動かします
    await page.evaluate('window.scrollTo(0, 500)')
    await asyncio.sleep(0.5)
    await page.evaluate('window.scrollTo(0, 0)')

    # 画像にあった「テーブル内のリンク」を狙い撃ち
    links_data = await page.evaluate('''() => {
        // テーブルの1列目付近にあるリンク(日付)をすべて取得
        const results = [];
        const anchors = document.querySelectorAll('table tr td a');
        anchors.forEach(a => {
            // URLに数字が含まれるもの（レポートページ）に限定
            if (a.href.match(/\\/\\d+\\/$/)) {
                results.push({ title: a.innerText.trim(), href: a.href });
            }
        });
        return results;
    }''')

    print(f"  [現場報告] 表の中から {len(links_data)} 件のリンクを検知しました。")

    valid_tasks = []
    limit_dt = datetime.strptime(LIMIT_DATE, "%Y-%m-%d")

    for item in links_data:
        # 日付形式(1/27や2025/1/27)が含まれているかチェック
        date_match = re.search(r'(\d{4}/)?\d{1,2}/\d{1,2}', item['title'])
        if not date_match: continue
        
        norm_date = normalize_date(date_match.group(0))
        if not norm_date or norm_date in existing_dates: continue
        
        # 期間チェック
        if datetime.strptime(norm_date, "%Y/%m/%d") < limit_dt: continue
        
        valid_tasks.append({"url": item['href'], "date": norm_date})
    
    return valid_tasks

# ==========================================
# BLOCK: 4. 強奪ロジック（1日の詳細を抜く：ここは実績あり）
# ==========================================
async def scrape_day_data(page, url):
    target_url = url + ("&" if "?" in url else "?") + "kishu=all"
    print(f"  [巡回] {target_url}")
    await page.goto(target_url, wait_until="load")
    await page.wait_for_timeout(5000) # マイナス数値反映待ち
    
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
        print("専用Chromeに接続します...")
        try:
            browser = await p.chromium.connect_over_cdp("http://localhost:9222")
            context = browser.contexts[0]
            
            # 【重要】リーダーが開いている「みんレポ」のタブを全タブから探し出す
            page = None
            for p_obj in context.pages:
                if "min-repo.com" in p_obj.url and "tag" in p_obj.url:
                    page = p_obj
                    break
            
            if not page:
                print("【エラー】『店舗トップページ（タグページ）』が見つかりません。")
                print("専用Chromeで対象店舗のリスト画面を開いておいてください。")
                return
            
            print(f"  [成功] ターゲットタブを特定しました: {await page.title()}")

        except Exception as e:
            print(f"接続エラー: {e}"); return

        for store in TARGET_STORES:
            print(f"\n=== {store['name']} 攻略開始 ===")
            try:
                sheet = gc.open_by_key(store['sheet_id'])
                cal_sheet = sheet.worksheet("カレンダー")
                raw_sheet = sheet.worksheet("生データ")
                existing_dates = cal_sheet.col_values(1)
                
                # 偵察（画像1, 2枚目の表をスキャン）
                new_tasks = await get_tasks_from_table(page, store['name'], existing_dates)
                print(f"--- 判定終了: 合計 {len(new_tasks)} 件の新規レポートを処理します ---")

                for task in sorted(new_tasks, key=lambda x: x['date'], reverse=True):
                    day_data = await scrape_day_data(page, task['url'])
                    if day_data:
                        rows = [[task['date'], r['name'], r['num'], clean_number(r['diff']), clean_number(r['games'])] for r in day_data]
                        raw_sheet.append_rows(rows)
                        t_diff = sum(r[3] for r in rows)
                        cal_sheet.append_row([task['date'], len(rows), t_diff, int(t_diff/len(rows)) if len(rows)>0 else 0, sum(r[4] for r in rows)])
                        print(f"    -> {task['date']} 書き込み完了（差枚: {t_diff}）")
                    
                    # 1店舗の日付を遡り続ける（リスト画面に戻る必要はないため、そのまま巡回）
                    await asyncio.sleep(2)

            except Exception as e:
                print(f"エラー: {e}")

if __name__ == "__main__":
    asyncio.run(main())