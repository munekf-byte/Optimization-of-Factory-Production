# --- VERSION: m_commander_v4_0_Pro_UI_Final ---
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import asyncio
import csv
import collections

# ==========================================
# BLOCK: 1. 固定設定
# ==========================================
SPREADSHEET_KEY = "1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I"
CONFIG_SHEET    = "分析設定"
CROSS_SHEET     = "クロス分析"
SINGLE_SHEET    = "機種別分析"
INDEX_SHEET     = "機種目録" 
LOCAL_DATABASE  = "/Users/macuser/Desktop/minrepo_project/minrepo_database.csv"

def hex_to_rgb(hex_str):
    hex_str = hex_str.lstrip('#')
    return {"red": int(hex_str[0:2], 16)/255.0, "green": int(hex_str[2:4], 16)/255.0, "blue": int(hex_str[4:6], 16)/255.0}

def get_number_format(mode):
    if mode == "機械割": return {"type": "NUMBER", "pattern": "0.00\"%\""}
    if mode == "G数": return {"type": "NUMBER", "pattern": "#,##0\"G\""}
    if mode == "粘り勝率": return {"type": "NUMBER", "pattern": "0.0\"%\""}
    return {"type": "NUMBER", "pattern": "#,##0\"枚\""}

# ==========================================
# BLOCK: 2. 高速同期エンジン
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
# BLOCK: 4. 機種別分析（v4.0 UI & Trash Collector）
# ==========================================
async def execute_single_analysis(doc, conf):
    print(f"   > 機種別分析: {conf['target_model']} 精密抽出中...")
    
    # --- STEP 1: 3/5日ルールによる正規台判定 ---
    unit_appearance = collections.defaultdict(list)
    raw_data = []
    with open(LOCAL_DATABASE, mode='r', encoding='utf-8-sig') as f:
        reader = csv.reader(f); next(reader, None)
        for row in reader:
            if len(row) < 6: continue
            d_date, d_store, d_model, d_unit, d_diff, d_games = [c.strip() for c in row]
            if not d_unit.isdigit() or conf['store'] not in d_store or conf['target_model'] not in d_model: continue
            dt = datetime.strptime(d_date, "%Y/%m/%d")
            unit_appearance[int(d_unit)].append(dt)
            raw_data.append({'date': d_date, 'unit': int(d_unit), 'diff': int(d_diff), 'games': int(d_games)})

    # 正規台の選別
    valid_units = []
    for u, dates in unit_appearance.items():
        dates.sort()
        is_valid = False
        if len(dates) >= 3:
            for i in range(len(dates) - 2):
                if (dates[i+2] - dates[i]).days <= 4: # 5日間のうちに3日出現
                    is_valid = True; break
        if is_valid: valid_units.append(u)
    
    if not valid_units: 
        print("   ! 条件に一致する正規台が見つかりませんでした。")
        return
    
    valid_units.sort()
    
    # --- STEP 2: データ集計 ---
    model_data = collections.defaultdict(dict)
    processed_keys = set()
    for entry in raw_data:
        if entry['unit'] not in valid_units: continue
        p_key = (entry['date'], entry['unit'])
        if p_key in processed_keys: continue
        processed_keys.add(p_key)
        model_data[entry['date']][entry['unit']] = {'diff': entry['diff'], 'games': entry['games']}

    sorted_dates = sorted(model_data.keys())
    dow_names = ["月", "火", "水", "木", "金", "土", "日"]
    
    # サマリー行の作成 (21-23行目)
    summary_10k, summary_5k, summary_avg = ["10,000枚突破率", "", "", "", "", "", ""], ["5,000枚突破率", "", "", "", "", "", ""], ["台別平均差枚", "", "", "", "", "", ""]
    for u in valid_units:
        diffs = [model_data[d][u]['diff'] for d in sorted_dates if u in model_data[d]]
        days = len(diffs)
        summary_10k.append(round(len([v for v in diffs if v>=10000])/days*100, 1) if days > 0 else 0)
        summary_5k.append(round(len([v for v in diffs if v>=5000])/days*100, 1) if days > 0 else 0)
        summary_avg.append(int(sum(diffs)/days) if days > 0 else 0)

    header = ["日付", "曜日", "総計", "台平均", "平均G", "機械割", "粘り勝率"] + [f"{u}番" for u in valid_units]
    data_rows = []
    for d_str in sorted_dates:
        day_data = model_data[d_str]
        t_d = sum(u['diff'] for u in day_data.values())
        t_g = sum(u['games'] for u in day_data.values())
        u_count = len(day_data)
        avg_d, avg_g = t_d/u_count, t_g/u_count
        m_rate = ((t_g * 3 + t_d) / (t_g * 3)) * 100 if t_g > 0 else 0
        sticky = (len([u for u in day_data.values() if u['games']>=5000 and u['diff']>0])/u_count) * 100
        
        row = [d_str, dow_names[datetime.strptime(d_str, "%Y/%m/%d").weekday()], t_d, int(avg_d), int(avg_g), m_rate, sticky]
        for u in valid_units:
            row.append(day_data[u]['diff'] if u in day_data else "")
        data_rows.append(row)

    # --- STEP 3: スプレッドシート書き込みとUI装飾 ---
    ws = doc.worksheet(SINGLE_SHEET); ws.clear()
    ws.update(values=[summary_10k, summary_5k, summary_avg, header], range_name='A21')
    ws.update(values=data_rows, range_name='A25')
    
    s_id = ws.id; l_row = len(data_rows) + 25; l_col = len(header)
    
    # リクエスト構築
    reqs = []
    # 1. 台番号ヘッダー(24行目)の装飾
    reqs.append({"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 23, "endRowIndex": 24, "startColumnIndex": 0, "endColumnIndex": l_col}, "cell": {"userEnteredFormat": {"backgroundColor": hex_to_rgb("#444444"), "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "bold": True}, "horizontalAlignment": "CENTER"}}, "fields": "userEnteredFormat"}})
    
    # 2. サマリー列(C:G)の単位と色
    summary_cols = [
        {"idx": 2, "fmt": get_number_format("差枚"), "color": "#fdf7f7"}, # 総計
        {"idx": 3, "fmt": get_number_format("差枚"), "color": "#fffcf0"}, # 台平均
        {"idx": 4, "fmt": get_number_format("G数"), "color": "#f0fdf0"}, # 平均G
        {"idx": 5, "fmt": get_number_format("機械割"), "color": "#f0f4ff"}, # 機械割
        {"idx": 6, "fmt": get_number_format("粘り勝率"), "color": "#fdf0ff"} # 粘り勝率
    ]
    for col in summary_cols:
        reqs.append({"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 24, "endRowIndex": l_row, "startColumnIndex": col['idx'], "endColumnIndex": col['idx']+1}, "cell": {"userEnteredFormat": {"numberFormat": col['fmt'], "backgroundColor": hex_to_rgb(col['color'])}}, "fields": "userEnteredFormat"}})

    # 3. 突破率グラデーション (21-22行目)
    for r_idx in [20, 21]:
        reqs.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": s_id, "startRowIndex": r_idx, "endRowIndex": r_idx+1, "startColumnIndex": 7, "endColumnIndex": l_col}], "gradientRule": {"minpoint": {"color": hex_to_rgb("#ffffff"), "type": "MIN"}, "maxpoint": {"color": hex_to_rgb("#0000ff"), "type": "MAX"}}}, "index": 0}})

    # 4. カレンダー色判定 (土曜=青, 日曜=赤)
    reqs.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": s_id, "startRowIndex": 24, "endRowIndex": l_row, "startColumnIndex": 1, "endColumnIndex": 2}], "booleanRule": {"condition": {"type": "TEXT_CONTAINS", "values": [{"userEnteredValue": "土"}]}, "format": {"textFormat": {"foregroundColor": {"blue": 1}}}}}, "index": 0}})
    reqs.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": s_id, "startRowIndex": 24, "endRowIndex": l_row, "startColumnIndex": 1, "endColumnIndex": 2}], "booleanRule": {"condition": {"type": "TEXT_CONTAINS", "values": [{"userEnteredValue": "日"}]}, "format": {"textFormat": {"foregroundColor": {"red": 1}}}}}, "index": 0}})

    doc.batch_update({"requests": reqs})
    print("   -> 機種別分析 v4.0 完了")

async def main():
    print(f"\n--- Ver.4.0 起動 (Pro UI & Trash Collector) ---")
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
    gc = gspread.authorize(creds); doc = gc.open_by_key(SPREADSHEET_KEY)
    await sync_store_list(doc)
    while True:
        try:
            conf_ws = doc.worksheet(CONFIG_SHEET); vals = conf_ws.get_all_values()
            all_cmd, single_cmd = vals[1][1], vals[7][2]
            if "実行" in str([all_cmd, single_cmd]):
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 指令受信。")
                conf = {"store": vals[4][1], "target_model": vals[7][1]}
                btn_cell = 'B2' if "実行" in all_cmd else 'C8'
                conf_ws.update_acell(btn_cell, "● 実行中")
                await execute_single_analysis(doc, conf)
                conf_ws.update_acell(btn_cell, "待機中")
            print(f"\r[{datetime.now().strftime('%H:%M:%S')}] STAND BY ...", end="")
        except Exception as e: print(f"\nError: {e}")
        await asyncio.sleep(15)

if __name__ == "__main__": asyncio.run(main())