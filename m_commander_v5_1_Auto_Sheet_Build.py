# --- VERSION: m_commander_v5_1_Auto_Sheet_Build ---
import gspread
from gspread.exceptions import WorksheetNotFound # 追加
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import asyncio
import csv
import collections
import jpholiday 

# ==========================================
# BLOCK: 1. 固定設定
# ==========================================
SPREADSHEET_KEY = "1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I"
CONFIG_SHEET    = "分析設定"
SINGLE_SHEET    = "機種別分析"
INDEX_SHEET     = "機種目録" 
LOCAL_DATABASE  = "/Users/macuser/Desktop/minrepo_project/minrepo_database.csv"

# ==========================================
# BLOCK: 2. 同期エンジン
# ==========================================
async def sync_store_list(doc):
    try:
        unique_stores = set()
        with open(LOCAL_DATABASE, mode='r', encoding='utf-8-sig') as f:
            reader = csv.reader(f); next(reader, None) 
            for row in reader:
                if len(row) > 1: unique_stores.add(row[1])
        stores = sorted(list(unique_stores))
        idx_ws = doc.worksheet(INDEX_SHEET)
        idx_ws.clear()
        idx_ws.update(values=[["店舗リスト(AutoSync)"]] + [[s] for s in stores], range_name='A1')
        print(f"   -> 店舗同期完了: {len(stores)}店舗。")
    except Exception as e: print(f"   ! 同期エラー: {e}")

# ==========================================
# BLOCK: 3. 機種別分析（v5.1 自動建築・純粋ロジック版）
# ==========================================
async def execute_single_analysis(doc, conf):
    print(f"   > 機種別分析: {conf['target_model']} 解析中...")
    
    # --- STEP 1: データ抽出 (3/5ルール適用) ---
    unit_appearance = collections.defaultdict(list); raw_data = []
    with open(LOCAL_DATABASE, mode='r', encoding='utf-8-sig') as f:
        reader = csv.reader(f); next(reader, None)
        for row in reader:
            if len(row) < 6: continue
            d_date, d_store, d_model, d_unit, d_diff, d_games = [c.strip() for c in row]
            if not d_unit.isdigit() or conf['store'] not in d_store or conf['target_model'] not in d_model: continue
            dt = datetime.strptime(d_date, "%Y/%m/%d")
            unit_appearance[int(d_unit)].append(dt)
            raw_data.append({'date': d_date, 'unit': int(d_unit), 'diff': int(d_diff), 'games': int(d_games)})

    valid_units = sorted([u for u, dates in unit_appearance.items() if any((sorted(dates)[i+2] - sorted(dates)[i]).days <= 4 for i in range(len(dates)-2))])
    if not valid_units: 
        print("   ! 正規台が見つかりませんでした。")
        return

    # --- STEP 2: 集計処理 ---
    model_data = collections.defaultdict(dict)
    unit_history = collections.defaultdict(list)
    dow_stats = collections.defaultdict(list)
    
    for entry in raw_data:
        if entry['unit'] in valid_units:
            dt = datetime.strptime(entry['date'], "%Y/%m/%d")
            model_data[entry['date']][entry['unit']] = {'diff': entry['diff'], 'games': entry['games']}
            unit_history[entry['unit']].append(entry['diff'])
            dow_stats[dt.weekday()].append(entry['diff'])

    sorted_dates = sorted(model_data.keys()); dow_names = ["月", "火", "水", "木", "金", "土", "日"]
    
    # 3-1. 店舗・機種サマリー
    total_diff = sum(sum(d['diff'] for d in day.values()) for day in model_data.values())
    total_games = sum(sum(d['games'] for d in day.values()) for day in model_data.values())
    overall_payout = ((total_games * 3 + total_diff) / (total_games * 3)) * 100 if total_games > 0 else 0
    report_top = [["【分析レポート】", conf['store'], conf['target_model']], ["期間", sorted_dates[0], "〜", sorted_dates[-1]], ["全体累計差枚", f"{total_diff}枚", "平均機械割", f"{overall_payout:.2f}%"]]

    # 3-2. 曜日別平均差枚
    dow_header = ["曜日別分析"] + dow_names
    dow_values = ["平均差枚"] + [int(sum(dow_stats[i])/len(dow_stats[i])) if dow_stats[i] else 0 for i in range(7)]
    
    # 3-3. 台別評価
    unit_summary_rows = [["台番号評価", "10k突破率", "5k突破率", "平均差枚"]]
    for u in valid_units:
        diffs = unit_history[u]; days = len(diffs)
        v10 = f"{len([v for v in diffs if v>=10000])/days*100:.1f}%" if days > 0 else "0.0%"
        v5 = f"{len([v for v in diffs if v>=5000])/days*100:.1f}%" if days > 0 else "0.0%"
        v_avg = int(sum(diffs)/days) if days > 0 else 0
        unit_summary_rows.append([f"{u}番", v10, v5, v_avg])

    # 4. 日別データエリア (C列はイベントログ予約)
    data_header = ["日付", "曜日", "イベントログ", "総計", "台平均", "平均G", "機械割", "粘り勝率"] + [f"{u}番" for u in valid_units]
    data_rows = []
    for d_str in sorted_dates:
        day_data = model_data[d_str]; u_count = len(day_data)
        t_d, t_g = sum(u['diff'] for u in day_data.values()), sum(u['games'] for u in day_data.values())
        avg_d, avg_g = t_d/u_count, t_g/u_count
        m_rate = f"{((t_g * 3 + t_d) / (t_g * 3)) * 100:.2f}%" if t_g > 0 else "0.00%"
        sticky = f"{(len([u for u in day_data.values() if u['games']>=5000 and u['diff']>0])/u_count)*100:.1f}%"
        row = [d_str, dow_names[datetime.strptime(d_str, "%Y/%m/%d").weekday()], "", t_d, int(avg_d), int(avg_g), m_rate, sticky]
        for u in valid_units: row.append(day_data[u]['diff'] if u in day_data else "")
        data_rows.append(row)

    # --- STEP 5: 建築 & 書き込み ---
    try:
        ws = doc.worksheet(SINGLE_SHEET)
    except WorksheetNotFound:
        print(f"   ! {SINGLE_SHEET} タブが見つかりません。自動作成します。")
        ws = doc.add_worksheet(title=SINGLE_SHEET, rows="2000", cols="100")
    
    ws.clear() # 書式は消さず、値のみクリア
    ws.update(values=report_top, range_name='A1')
    ws.update(values=[dow_header, dow_values], range_name='A5')
    ws.update(values=unit_summary_rows, range_name='A8')
    ws.update(values=[data_header] + data_rows, range_name='A20')
    
    print("\n   -> 機種別分析 v5.1 完了 (Auto Build)")

async def main():
    print(f"\n--- Ver.5.1 起動 (Pure Logic & Auto Build) ---")
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
    gc = gspread.authorize(creds); doc = gc.open_by_key(SPREADSHEET_KEY)
    await sync_store_list(doc)
    while True:
        try:
            conf_ws = doc.worksheet(CONFIG_SHEET); vals = conf_ws.get_all_values()
            if "実行" in str([vals[1][1], vals[7][2]]):
                btn = 'B2' if "実行" in vals[1][1] else 'C8'
                conf_ws.update_acell(btn, "● 実行中")
                await execute_single_analysis(doc, {"store": vals[4][1], "target_model": vals[7][1]})
                conf_ws.update_acell(btn, "待機中")
            print(f"\r[{datetime.now().strftime('%H:%M:%S')}] STAND BY ...", end="")
        except Exception as e: print(f"\nError: {e}")
        await asyncio.sleep(15)

if __name__ == "__main__": asyncio.run(main())