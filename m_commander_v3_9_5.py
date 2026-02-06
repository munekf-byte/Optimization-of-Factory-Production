# --- VERSION: m_commander_v3_9_5_HighSpeed_Stream_20260205 ---
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
    return {"type": "NUMBER", "pattern": "#,##0\"枚\""}

# ==========================================
# BLOCK: 2. 超高速同期エンジン（メモリ節約型）
# ==========================================
async def sync_store_list(doc):
    print(f"   > [{datetime.now().strftime('%H:%M:%S')}] 倉庫(30万行)を高速スキャン中...")
    try:
        unique_stores = set()
        # メモリを食わないよう、1行ずつストリーム読み込み
        with open(LOCAL_DATABASE, mode='r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            next(reader, None) 
            for row in reader:
                if len(row) > 1: unique_stores.add(row[1])
        
        stores = sorted(list(unique_stores))
        idx_ws = doc.worksheet(INDEX_SHEET)
        idx_ws.clear()
        idx_ws.update(values=[["店舗リスト(AutoSync)"]] + [[s] for s in stores], range_name='A1')
        
        conf_ws = doc.worksheet(CONFIG_SHEET)
        range_formula = f"='{INDEX_SHEET}'!$A$2:$A${len(stores) + 1}"
        req = {
            "setDataValidation": {
                "range": {"sheetId": conf_ws.id, "startRowIndex": 4, "endRowIndex": 5, "startColumnIndex": 1, "endColumnIndex": 2},
                "rule": {
                    "condition": {"type": "VALUE_IN_RANGE", "values": [{"userEnteredValue": range_formula}]},
                    "showCustomUi": True, "strict": True
                }
            }
        }
        doc.batch_update({"requests": [req]})
        print(f"   -> 同期完了: {len(stores)}店舗。")
    except Exception as e:
        print(f"   ! 同期エラー: {e}")

# ==========================================
# BLOCK: 3. クロス分析（ストリーム読み込み版）
# ==========================================
async def execute_cross_analysis(doc, conf):
    print("   > クロス分析: 軽量抽出中...")
    daily_stats = collections.defaultdict(lambda: {'all': [], 'A': [], 'B': [], 'C': []})
    processed_keys = set()
    
    with open(LOCAL_DATABASE, mode='r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            if len(row) < 6: continue
            d_date, d_store, d_model, d_unit, d_diff, d_games = [c.strip() for c in row]
            if not d_unit.isdigit() or conf['store'] not in d_store: continue
            
            p_key = (d_date, d_unit) # 重複排除
            if p_key in processed_keys: continue
            processed_keys.add(p_key)
            
            entry = {'diff': int(d_diff), 'games': int(d_games)}
            daily_stats[d_date]['all'].append(entry)
            clean_m = d_model.replace("[撤去] ", "")
            if any(clean_m == m for m in conf['A_list']): daily_stats[d_date]['A'].append(entry)
            if any(clean_m == m for m in conf['B_list']): daily_stats[d_date]['B'].append(entry)
            if any(clean_m == m for m in conf['C_list']): daily_stats[d_date]['C'].append(entry)

    sorted_dates = sorted(daily_stats.keys())
    if not sorted_dates: return
    mid_ref = 0 if conf['mode'] == "差枚" else 4000 if conf['mode'] == "G数" else 100
    
    base_vals = {'all': [], 'A': [], 'B': [], 'C': []}
    for d_str in sorted_dates:
        for k in base_vals.keys():
            active = [e for e in daily_stats[d_str][k] if e['games'] > 0]
            val = mid_ref
            if active:
                if conf['mode'] == "差枚": val = sum(e['diff'] for e in active) / len(active)
                elif conf['mode'] == "G数": val = sum(e['games'] for e in active) / len(active)
                else:
                    t_d, t_g = sum(e['diff'] for e in active), sum(e['games'] for e in active)
                    val = ((t_g * 3 + t_d) / (t_g * 3) * 100) if t_g > 0 else 100
            base_vals[k].append(round(val, 2))

    windows = [7, 15, 30]
    ma_results = {w: {k: [round(v, 2) for v in (lambda data, win: [sum(data[max(0, i-win+1):i+1])/len(data[max(0, i-win+1):i+1]) for i in range(len(data))])(base_vals[k], w)] for k in base_vals.keys()} for w in windows}

    ws = doc.worksheet(CROSS_SHEET); ws.clear()
    full_ws = doc.fetch_sheet_metadata()
    current_sheet = next(s for s in full_ws['sheets'] if s['properties']['title'] == CROSS_SHEET)
    charts = current_sheet.get('charts', [])
    if charts: doc.batch_update({"requests": [{"deleteEmbeddedObject": {"objectId": c['chartId']}} for c in charts]})

    header_main = ["日付", "曜日", "店全体", conf['A_name'], conf['B_name'], conf['C_name']]
    for w in windows: header_main += [f"店({w})", f"A({w})", f"B({w})", f"C({w})"]
    data_rows = [[d, ["月", "火", "水", "木", "金", "土", "日"][datetime.strptime(d, "%Y/%m/%d").weekday()]] + [base_vals[k][i] for k in ['all', 'A', 'B', 'C']] + [ma_results[w][k][i] for w in windows for k in ['all', 'A', 'B', 'C']] for i, d in enumerate(sorted_dates)]
    
    ws.update(values=[header_main] + data_rows, range_name='A80')
    ws.update(values=data_rows, range_name='U80')

    s_id = ws.id; l_row = len(data_rows) + 80
    def build_panorama_chart(w_idx, window_val, anchor_row):
        start_col = 22 + (w_idx * 4) 
        title = f"【{window_val}日MA】灰:全体 赤:{conf['A_name']} 青:{conf['B_name']} 緑:{conf['C_name']}"
        series = []
        colors = ["#999999", "#ff0000", "#0000ff", "#00ff00"]
        for i, c in enumerate(range(start_col, start_col + 4)):
            series.append({"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 79, "endRowIndex": l_row, "startColumnIndex": c, "endColumnIndex": c+1}]}}, "targetAxis": "LEFT_AXIS", "color": hex_to_rgb(colors[i]), "lineStyle": {"width": 2 if i>0 else 4}})
        return {"addChart": {"chart": {"spec": {"title": title, "basicChart": {"chartType": "LINE", "legendPosition": "BOTTOM_LEGEND", "axis": [{"position": "BOTTOM_AXIS"}, {"position": "LEFT_AXIS"}], "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 79, "endRowIndex": l_row, "startColumnIndex": 20, "endColumnIndex": 21}]}}}], "series": series}}, "position": {"overlayPosition": {"anchorCell": {"sheetId": s_id, "rowIndex": anchor_row, "columnIndex": 6}, "widthPixels": 5000, "heightPixels": 450}}}}}

    reqs = [
        {"updateDimensionProperties": {"range": {"sheetId": s_id, "dimension": "COLUMNS", "startIndex": 6, "endIndex": 30}, "properties": {"pixelSize": 250}, "fields": "pixelSize"}},
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 79, "endRowIndex": l_row, "startColumnIndex": 2, "endColumnIndex": 18}, "cell": {"userEnteredFormat": {"numberFormat": get_number_format(conf['mode']), "horizontalAlignment": "CENTER"}}, "fields": "userEnteredFormat"}},
        build_panorama_chart(0, 7, 0), build_panorama_chart(1, 15, 25), build_panorama_chart(2, 30, 50)
    ]
    doc.batch_update({"requests": reqs})
    print("   -> クロス分析完了")

# ==========================================
# BLOCK: 4. 機種別分析（軍師＆クリーンUI）
# ==========================================
async def execute_single_analysis(doc, conf):
    print(f"   > 機種別分析: {conf['target_model']} 精密抽出中...")
    model_data = collections.defaultdict(dict)
    unique_units, processed_keys = set(), set()
    weekday_stats = collections.defaultdict(list)
    
    with open(LOCAL_DATABASE, mode='r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            if len(row) < 6: continue
            d_date, d_store, d_model, d_unit, d_diff, d_games = [c.strip() for c in row]
            if not d_unit.isdigit() or conf['store'] not in d_store or conf['target_model'] not in d_model: continue
            
            p_key = (d_date, d_unit)
            if p_key in processed_keys: continue
            processed_keys.add(p_key)

            u_int, d_int, g_int = int(d_unit), int(d_diff), int(d_games)
            unique_units.add(u_int)
            model_data[d_date][u_int] = {'diff': d_int, 'games': g_int}
            weekday_stats[datetime.strptime(d_date, "%Y/%m/%d").weekday()].append(d_int)

    if not unique_units: return
    sorted_units = sorted(list(unique_units)); sorted_dates = sorted(model_data.keys())
    dow_names = ["月", "火", "水", "木", "金", "土", "日"]
    
    # サマリー計算 (H列以降)
    summary_10k = ["10,000枚突破率", ""] + [""] * 5
    summary_5k  = ["5,000枚突破率", ""] + [""] * 5
    summary_avg = ["台別平均差枚", ""] + [""] * 5
    for u in sorted_units:
        diffs = [model_data[d][u]['diff'] for d in sorted_dates if u in model_data[d]]
        days = len(diffs)
        summary_10k.append(f"{round(len([v for v in diffs if v>=10000])/days*100, 1)}%" if days > 0 else "0%")
        summary_5k.append(f"{round(len([v for v in diffs if v>=5000])/days*100, 1)}%" if days > 0 else "0%")
        summary_avg.append(int(sum(diffs)/days) if days > 0 else 0)

    header = ["日付", "曜日", "総計", "台平均", "平均G", "機械割", "粘り勝率"] + [f"{u}番台" for u in sorted_units]
    data_rows = []
    for d_str in sorted_dates:
        day = model_data[d_str]
        t_d, t_g = sum(u['diff'] for u in day.values()), sum(u['games'] for u in day.values())
        avg_d, avg_g = t_d/len(day), t_g/len(day)
        m_rate = ((t_g * 3 + t_d) / (t_g * 3)) * 100 if t_g > 0 else 0
        sticky = len([u for u in day.values() if u['games']>=5000 and u['diff']>0])/len(day)
        data_rows.append([d_str, dow_names[datetime.strptime(d_str, "%Y/%m/%d").weekday()], t_d, int(avg_d), int(avg_g), m_rate, sticky] + [day[u]['diff'] if u in day else "" for u in sorted_units])

    ws = doc.worksheet(SINGLE_SHEET); ws.clear()
    ws.update(values=[summary_10k, summary_5k, summary_avg, header], range_name='A21')
    ws.update(values=data_rows, range_name='A25')
    
    s_id = ws.id; l_row = len(data_rows) + 25; l_col = len(header)
    reqs = [
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 20, "endRowIndex": 23}, "cell": {"userEnteredFormat": {"backgroundColor": hex_to_rgb("#f3f3f3"), "textFormat": {"bold": True}}}, "fields": "userEnteredFormat"}},
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 24, "endRowIndex": l_row, "startColumnIndex": 2, "endColumnIndex": 4}, "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0\"枚\""}}}, "fields": "userEnteredFormat"}},
    ]
    # 空欄グレー＆色彩ルール
    t_range = {"sheetId": s_id, "startRowIndex": 21, "endRowIndex": l_row, "startColumnIndex": 7, "endColumnIndex": l_col}
    reqs.append({"addConditionalFormatRule": {"rule": {"ranges": [t_range], "booleanRule": {"condition": {"type": "BLANK"}, "format": {"backgroundColor": hex_to_rgb("#efefef")}}}, "index": 0}})
    
    doc.batch_update({"requests": reqs})
    print("   -> 機種別分析 完了")

async def main():
    print(f"\n--- Ver.3.9.5 起動 (Memory Safe Edition) ---")
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
    gc = gspread.authorize(creds); doc = gc.open_by_key(SPREADSHEET_KEY)
    await sync_store_list(doc)
    while True:
        try:
            conf_ws = doc.worksheet(CONFIG_SHEET)
            conf_ws.update_acell('D2', f"監視中: {datetime.now().strftime('%H:%M:%S')}")
            vals = conf_ws.get_all_values()
            all_cmd, single_cmd, cross_cmd = vals[1][1], vals[7][2], vals[9][2]
            if "実行" in str([all_cmd, single_cmd, cross_cmd]):
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 指令。")
                conf = {
                    "store": vals[4][1], "mode": vals[9][1], "target_model": vals[7][1],
                    "A_name": vals[10][1], "A_list": [v[1] for v in vals[11:23] if v[1]],
                    "B_name": vals[23][1], "B_list": [v[1] for v in vals[24:36] if v[1]],
                    "C_name": vals[36][1], "C_list": [v[1] for v in vals[37:49] if v[1]]
                }
                btn_cell = 'B2' if "実行" in all_cmd else ('C8' if "実行" in single_cmd else 'C10')
                conf_ws.update_acell(btn_cell, "● 実行中")
                if "実行" in all_cmd or "実行" in cross_cmd: await execute_cross_analysis(doc, conf)
                if "実行" in all_cmd or "実行" in single_cmd: await execute_single_analysis(doc, conf)
                conf_ws.update_acell(btn_cell, "待機中")
        except Exception as e: print(f"Error: {e}")
        await asyncio.sleep(15)

if __name__ == "__main__": asyncio.run(main())