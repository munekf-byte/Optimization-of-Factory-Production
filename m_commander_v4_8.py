# --- VERSION: m_commander_v4_8 ---
import gspread
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

def hex_to_rgb(hex_str):
    hex_str = hex_str.lstrip('#')
    return {"red": int(hex_str[0:2], 16)/255.0, "green": int(hex_str[2:4], 16)/255.0, "blue": int(hex_str[4:6], 16)/255.0}

def get_number_format(mode):
    if mode == "機械割": return {"type": "NUMBER", "pattern": "0.00\"%\""}
    if mode == "G数": return {"type": "NUMBER", "pattern": "#,##0\"G\""}
    if mode == "粘り勝率": return {"type": "NUMBER", "pattern": "0.0\"%\""}
    return {"type": "NUMBER", "pattern": "#,##0\"枚\""}

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
# BLOCK: 3. 機種別分析（v4.8 完全UI版）
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
    if not valid_units: return

    # --- STEP 2: 集計準備 ---
    model_data = collections.defaultdict(dict)
    unit_history = collections.defaultdict(list) # 5段階評価用
    for entry in raw_data:
        if entry['unit'] in valid_units:
            model_data[entry['date']][entry['unit']] = {'diff': entry['diff'], 'games': entry['games']}
            unit_history[entry['unit']].append(entry['diff'])

    sorted_dates = sorted(model_data.keys()); dow_names = ["月", "火", "水", "木", "金", "土", "日"]
    
    # サマリー行作成
    summary_10k, summary_5k, summary_avg = ["10,000枚突破率", "", "", "", "", "", ""], ["5,000枚突破率", "", "", "", "", "", ""], ["台別平均差枚", "", "", "", "", "", ""]
    all_summary_vals = {'10k': [], '5k': []}
    for u in valid_units:
        diffs = unit_history[u]; days = len(diffs)
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

    # --- STEP 3: シート再構築 (Nuclear Reset) ---
    try: ws = doc.worksheet(SINGLE_SHEET); doc.del_worksheet(ws)
    except: pass
    ws = doc.add_worksheet(title=SINGLE_SHEET, rows=2000, cols=200); s_id = ws.id
    
    # データの流し込み
    ws.update(values=[summary_10k, summary_5k, summary_avg, ["日付", "曜日", "総計", "台平均", "平均G", "機械割", "粘り勝率"] + [f"{u}番" for u in valid_units]], range_name='A21')
    ws.update(values=data_rows, range_name='A25')

    # --- STEP 4: UI構築命令集 (BatchUpdate) ---
    reqs = []
    l_row = len(data_rows) + 25; l_col = len(valid_units) + 7

    # 1. 基本設定：H列以降を中央揃え
    reqs.append({"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 20, "endRowIndex": l_row, "startColumnIndex": 7, "endColumnIndex": l_col}, "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"}}, "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment)"}})

    # 2. 突破率エリア(21-22行目)のデザイン
    for r_idx, vals in [(20, all_summary_vals['10k']), (21, all_summary_vals['5k'])]:
        t_range = {"sheetId": s_id, "startRowIndex": r_idx, "endRowIndex": r_idx+1, "startColumnIndex": 7, "endColumnIndex": l_col}
        if vals:
            threshold = sorted(vals)[-max(1, int(len(vals)*0.3))]
            # 白太字ルール（最優先）
            reqs.append({"addConditionalFormatRule": {"rule": {"ranges": [t_range], "booleanRule": {"condition": {"type": "NUMBER_GREATER_THAN_EQ", "values": [{"userEnteredValue": str(threshold)}]}, "format": {"textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "bold": True}}}}, "index": 0}})
            # グラデーション
            reqs.append({"addConditionalFormatRule": {"rule": {"ranges": [t_range], "gradientRule": {"minpoint": {"color": hex_to_rgb("#ffffff"), "type": "MIN"}, "maxpoint": {"color": hex_to_rgb("#0000ff"), "type": "MAX"}}}, "index": 1}})

    # 3. 個別台データ(H25以降)の5段階評価 ＆ 空白グレー
    for i, u in enumerate(valid_units):
        c_idx = i + 7
        u_vals = [v for v in unit_history[u] if v is not None]
        v_min, v_max = min(u_vals), max(u_vals)
        v_range = (v_max - v_min) or 1
        steps = [v_min + v_range * p for p in [0.2, 0.4, 0.6, 0.8]]
        u_range = {"sheetId": s_id, "startRowIndex": 24, "endRowIndex": l_row, "startColumnIndex": c_idx, "endColumnIndex": c_idx+1}
        # ルール適用 (優先順位順)
        fmt_set = [
            ("IS_BLANK", [], None, "#efefef", False), # 非稼働
            ("NUMBER_EQ", [str(v_max)], "#0000ff", None, True), # 最上層
            ("NUMBER_GREATER_THAN_EQ", [str(steps[3])], "#0000ff", None, False), # 上層
            ("NUMBER_LESS_THAN_EQ", [str(steps[0])], "#ff0000", None, True), # 最下層
            ("NUMBER_LESS_THAN_EQ", [str(steps[1])], "#ff0000", None, False), # 下層
        ]
        for cond, vals, t_color, b_color, bold in fmt_set:
            fmt = {"textFormat": {"bold": bold}}
            if t_color: fmt["textFormat"]["foregroundColor"] = hex_to_rgb(t_color)
            if b_color: fmt["backgroundColor"] = hex_to_rgb(b_color)
            reqs.append({"addConditionalFormatRule": {"rule": {"ranges": [u_range], "booleanRule": {"condition": {"type": cond, "values": [{"userEnteredValue": v} for v in vals]}, "format": fmt}}, "index": 0}})

    # 4. サマリー(C-G列) & 機械割 (FIX済みロジック)
    f_range = {"sheetId": s_id, "startRowIndex": 24, "endRowIndex": l_row, "startColumnIndex": 5, "endColumnIndex": 6}
    rules = [("NUMBER_LESS", ["90"], "#ff0000", True), ("NUMBER_BETWEEN", ["90", "100"], "#ff0000", False), ("NUMBER_BETWEEN", ["100", "103"], "#000000", False), ("NUMBER_BETWEEN", ["103", "110"], "#0000ff", False), ("NUMBER_GREATER_THAN_EQ", ["110"], "#0000ff", True)]
    for cond, vals, color, bold in rules:
        reqs.append({"addConditionalFormatRule": {"rule": {"ranges": [f_range], "booleanRule": {"condition": {"type": cond, "values": [{"userEnteredValue": v} for v in vals]}, "format": {"textFormat": {"foregroundColor": hex_to_rgb(color), "bold": bold}}}}, "index": 0}})

    # 5. カレンダー(A-B列) 祝日・振替休日対応
    for i, d_str in enumerate(sorted_dates):
        dt = datetime.strptime(d_str, "%Y/%m/%d")
        color = {"red": 1, "green": 0, "blue": 0} if dt.weekday()==6 or jpholiday.is_holiday(dt) else ({"red": 0, "green": 0, "blue": 1} if dt.weekday()==5 else {"red": 0, "green": 0, "blue": 0})
        reqs.append({"updateCells": {"range": {"sheetId": s_id, "startRowIndex": 24+i, "endRowIndex": 25+i, "startColumnIndex": 0, "endColumnIndex": 2}, "rows": [{"values": [{"userEnteredFormat": {"textFormat": {"foregroundColor": color}}}, {"userEnteredFormat": {"textFormat": {"foregroundColor": color}}}]}], "fields": "userEnteredFormat.textFormat.foregroundColor"}})

    # ヘッダーデザイン & 単位
    reqs.append({"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 23, "endRowIndex": 24, "startColumnIndex": 0, "endColumnIndex": l_col}, "cell": {"userEnteredFormat": {"backgroundColor": hex_to_rgb("#444444"), "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "bold": True}, "horizontalAlignment": "CENTER"}}, "fields": "userEnteredFormat"}})
    for i, mode in [(2,"差枚"),(3,"差枚"),(4,"G数"),(5,"機械割"),(6,"粘り勝率")]:
        reqs.append({"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 24, "endRowIndex": l_row, "startColumnIndex": i, "endColumnIndex": i+1}, "cell": {"userEnteredFormat": {"numberFormat": get_number_format(mode)}}, "fields": "userEnteredFormat"}})

    doc.batch_update({"requests": reqs})
    print("\n   -> 機種別分析 v4.8 完了 (UI再定義・完結)")

async def main():
    print(f"\n--- Ver.4.8 起動 (Final UI Definition) ---")
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
    gc = gspread.authorize(creds); doc = gc.open_by_key(SPREADSHEET_KEY)
    await sync_store_list(doc)
    while True:
        try:
            conf_ws = doc.worksheet(CONFIG_SHEET); vals = conf_ws.get_all_values()
            if "実行" in str([vals[1][1], vals[7][2]]):
                cell = 'B2' if "実行" in vals[1][1] else 'C8'
                conf_ws.update_acell(cell, "● 実行中")
                await execute_single_analysis(doc, {"store": vals[4][1], "target_model": vals[7][1]})
                conf_ws.update_acell(cell, "待機中")
            print(f"\r[{datetime.now().strftime('%H:%M:%S')}] STAND BY ...", end="")
        except Exception as e: print(f"\nError: {e}")
        await asyncio.sleep(15)

if __name__ == "__main__": asyncio.run(main())