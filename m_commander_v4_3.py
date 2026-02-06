# --- VERSION: m_commander_v4_3 ---
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import asyncio
import csv
import collections

# ==========================================
# BLOCK: 1. 固定設定
# ==========================================
SPREADSHEET_KEY = "1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I"
CONFIG_SHEET    = "分析設定"
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
# BLOCK: 3. 機種別分析（v4.3 UI完全版・API Fix）
# ==========================================
async def execute_single_analysis(doc, conf):
    print(f"   > 機種別分析: {conf['target_model']} 解析中...")
    
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

    valid_units = [u for u, dates in unit_appearance.items() if any((sorted(dates)[i+2] - sorted(dates)[i]).days <= 4 for i in range(len(dates)-2))]
    if not valid_units: 
        print("   ! 正規台が見つかりませんでした。")
        return
    valid_units.sort()
    
    # --- STEP 2: データ集計 ---
    model_data = collections.defaultdict(dict)
    for entry in raw_data:
        if entry['unit'] in valid_units:
            model_data[entry['date']][entry['unit']] = {'diff': entry['diff'], 'games': entry['games']}

    sorted_dates = sorted(model_data.keys())
    dow_names = ["月", "火", "水", "木", "金", "土", "日"]
    
    summary_10k, summary_5k, summary_avg = ["10,000枚突破率", "", "", "", "", "", ""], ["5,000枚突破率", "", "", "", "", "", ""], ["台別平均差枚", "", "", "", "", "", ""]
    all_summary_vals = {'10k': [], '5k': []}
    for u in valid_units:
        diffs = [model_data[d][u]['diff'] for d in sorted_dates if u in model_data[d]]
        days = len(diffs)
        v10 = round(len([v for v in diffs if v>=10000])/days*100, 1) if days > 0 else 0
        v5 = round(len([v for v in diffs if v>=5000])/days*100, 1) if days > 0 else 0
        summary_10k.append(v10); summary_5k.append(v5); summary_avg.append(int(sum(diffs)/days) if days > 0 else 0)
        all_summary_vals['10k'].append(v10); all_summary_vals['5k'].append(v5)

    data_rows, col_vals = [], {'diff_total': [], 'diff_avg': [], 'games_avg': [], 'win_rate': []}
    for d_str in sorted_dates:
        day_data = model_data[d_str]; u_count = len(day_data)
        t_d, t_g = sum(u['diff'] for u in day_data.values()), sum(u['games'] for u in day_data.values())
        avg_d, avg_g = t_d/u_count, t_g/u_count
        m_rate = ((t_g * 3 + t_d) / (t_g * 3)) * 100 if t_g > 0 else 0
        sticky = (len([u for u in day_data.values() if u['games']>=5000 and u['diff']>0])/u_count) * 100
        row = [d_str, dow_names[datetime.strptime(d_str, "%Y/%m/%d").weekday()], t_d, int(avg_d), int(avg_g), m_rate, sticky]
        for u in valid_units: row.append(day_data[u]['diff'] if u in day_data else "")
        data_rows.append(row)
        col_vals['diff_total'].append(t_d); col_vals['diff_avg'].append(avg_d); col_vals['games_avg'].append(avg_g); col_vals['win_rate'].append(sticky)

    ws = doc.worksheet(SINGLE_SHEET); ws.clear()
    ws.update(values=[summary_10k, summary_5k, summary_avg, ["日付", "曜日", "総計", "台平均", "平均G", "機械割", "粘り勝率"] + [f"{u}番" for u in valid_units]], range_name='A21')
    ws.update(values=data_rows, range_name='A25')
    
    s_id = ws.id; l_row = len(data_rows) + 25; l_col = len(valid_units) + 7
    reqs = [{"updateCells": {"range": {"sheetId": s_id, "startRowIndex": 20, "endRowIndex": l_row, "startColumnIndex": 0, "endColumnIndex": l_col}, "fields": "userEnteredFormat"}}] # 書式リセット

    # UI装飾1: ヘッダー
    reqs.append({"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 23, "endRowIndex": 24, "startColumnIndex": 0, "endColumnIndex": l_col}, "cell": {"userEnteredFormat": {"backgroundColor": hex_to_rgb("#444444"), "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "bold": True}, "horizontalAlignment": "CENTER"}}, "fields": "userEnteredFormat"}})

    # UI装飾2: 突破率グラデーション + 白抜き文字(THAN_EQ)
    for r_idx, vals in [(20, all_summary_vals['10k']), (21, all_summary_vals['5k'])]:
        reqs.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": s_id, "startRowIndex": r_idx, "endRowIndex": r_idx+1, "startColumnIndex": 7, "endColumnIndex": l_col}], "gradientRule": {"minpoint": {"color": hex_to_rgb("#ffffff"), "type": "MIN"}, "maxpoint": {"color": hex_to_rgb("#0000ff"), "type": "MAX"}}}, "index": 0}})
        if vals:
            threshold = sorted(vals)[-max(1, int(len(vals)*0.3))]
            reqs.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": s_id, "startRowIndex": r_idx, "endRowIndex": r_idx+1, "startColumnIndex": 7, "endColumnIndex": l_col}], "booleanRule": {"condition": {"type": "NUMBER_GREATER_THAN_EQ", "values": [{"userEnteredValue": str(threshold)}]}, "format": {"textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "bold": True}}}}, "index": 0}})

    # UI装飾3: 機械割 (絶対評価ロジック)
    f_range = {"sheetId": s_id, "startRowIndex": 24, "endRowIndex": l_row, "startColumnIndex": 5, "endColumnIndex": 6}
    rules = [("NUMBER_LESS", ["90"], "#ff0000", True), ("NUMBER_BETWEEN", ["90", "100"], "#ff0000", False), ("NUMBER_BETWEEN", ["100", "103"], "#000000", False), ("NUMBER_BETWEEN", ["103", "110"], "#0000ff", False), ("NUMBER_GREATER_THAN_EQ", ["110"], "#0000ff", True)]
    for cond, vals, color, bold in rules:
        reqs.append({"addConditionalFormatRule": {"rule": {"ranges": [f_range], "booleanRule": {"condition": {"type": cond, "values": [{"userEnteredValue": v} for v in vals]}, "format": {"textFormat": {"foregroundColor": hex_to_rgb(color), "bold": bold}}}}, "index": 0}})

    # UI装飾4: サマリー列 相対5段階(THAN_EQ / LESS_EQ)
    for c_idx, key in [(2, 'diff_total'), (3, 'diff_avg'), (4, 'games_avg'), (6, 'win_rate')]:
        v_min, v_max = min(col_vals[key]), max(col_vals[key])
        v_range = (v_max - v_min) or 1
        steps = [v_min + v_range * p for p in [0.2, 0.4, 0.6, 0.8]]
        c_range = {"sheetId": s_id, "startRowIndex": 24, "endRowIndex": l_row, "startColumnIndex": c_idx, "endColumnIndex": c_idx+1}
        for cond, vals, color, bold in [("NUMBER_LESS_THAN_EQ", [str(steps[0])], "#ff0000", True), ("NUMBER_BETWEEN", [str(steps[0]), str(steps[1])], "#ff0000", False), ("NUMBER_BETWEEN", [str(steps[1]), str(steps[2])], "#000000", False), ("NUMBER_BETWEEN", [str(steps[2]), str(steps[3])], "#0000ff", False), ("NUMBER_GREATER_THAN_EQ", [str(steps[3])], "#0000ff", True)]:
            reqs.append({"addConditionalFormatRule": {"rule": {"ranges": [c_range], "booleanRule": {"condition": {"type": cond, "values": [{"userEnteredValue": v} for v in vals]}, "format": {"textFormat": {"foregroundColor": hex_to_rgb(color), "bold": bold}}}}, "index": 0}})

    # 曜日色と単位
    reqs.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": s_id, "startRowIndex": 24, "endRowIndex": l_row, "startColumnIndex": 1, "endColumnIndex": 2}], "booleanRule": {"condition": {"type": "TEXT_CONTAINS", "values": [{"userEnteredValue": "土"}]}, "format": {"textFormat": {"foregroundColor": {"blue": 1}}}}}, "index": 0}})
    reqs.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": s_id, "startRowIndex": 24, "endRowIndex": l_row, "startColumnIndex": 1, "endColumnIndex": 2}], "booleanRule": {"condition": {"type": "TEXT_CONTAINS", "values": [{"userEnteredValue": "日"}]}, "format": {"textFormat": {"foregroundColor": {"red": 1}}}}}, "index": 0}})
    for i, mode in [(2,"差枚"),(3,"差枚"),(4,"G数"),(5,"機械割"),(6,"粘り勝率")]:
        reqs.append({"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 24, "endRowIndex": l_row, "startColumnIndex": i, "endColumnIndex": i+1}, "cell": {"userEnteredFormat": {"numberFormat": get_number_format(mode)}}, "fields": "userEnteredFormat"}})

    doc.batch_update({"requests": reqs})
    print("\n   -> 機種別分析 v4.3 完了")

async def main():
    print(f"\n--- Ver.4.3 起動 (Stable UI Edition) ---")
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
    gc = gspread.authorize(creds); doc = gc.open_by_key(SPREADSHEET_KEY)
    await sync_store_list(doc)
    while True:
        try:
            conf_ws = doc.worksheet(CONFIG_SHEET); vals = conf_ws.get_all_values()
            if "実行" in str([vals[1][1], vals[7][2]]):
                btn_cell = 'B2' if "実行" in vals[1][1] else 'C8'
                conf_ws.update_acell(btn_cell, "● 実行中")
                await execute_single_analysis(doc, {"store": vals[4][1], "target_model": vals[7][1]})
                conf_ws.update_acell(btn_cell, "待機中")
            print(f"\r[{datetime.now().strftime('%H:%M:%S')}] STAND BY ...", end="")
        except Exception as e: print(f"\nError: {e}")
        await asyncio.sleep(15)

if __name__ == "__main__": asyncio.run(main())