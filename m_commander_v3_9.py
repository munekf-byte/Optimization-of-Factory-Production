# --- VERSION: m_commander_v3_9_Panorama_UI_Master_20260202 ---

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import asyncio
import csv

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
# BLOCK: 2. 自動同期エンジン（先行起動型）
# ==========================================
async def sync_store_list(doc):
    print("   > 店舗リストを倉庫から同期中...")
    try:
        with open(LOCAL_DATABASE, mode='r', encoding='utf-8-sig') as f:
            all_data = list(csv.reader(f))
        
        unique_stores = sorted(list(set(row[1] for row in all_data[1:] if len(row) > 1)))
        
        # B5プルダウンを直接更新 (ONE_OF_LIST方式)
        conf_ws = doc.worksheet(CONFIG_SHEET)
        req = {
            "setDataValidation": {
                "range": {"sheetId": conf_ws.id, "startRowIndex": 4, "endRowIndex": 5, "startColumnIndex": 1, "endColumnIndex": 2},
                "rule": {
                    "condition": {"type": "ONE_OF_LIST", "values": [{"userEnteredValue": s} for s in unique_stores]},
                    "showCustomUi": True, "strict": True
                }
            }
        }
        doc.batch_update({"requests": [req]})
        print(f"   -> 同期完了: {len(unique_stores)}店舗。B5セルが最新です。")
        return all_data
    except Exception as e:
        print(f"   ! 店舗同期エラー: {e}")
        return None

# ==========================================
# BLOCK: 3. クロス分析エンジン（UI強化版）
# ==========================================
async def execute_cross_analysis(doc, conf, all_data):
    print("   > クロス分析: UIレイアウト構築中...")
    
    def get_axis_limits(mode, window):
        if mode == "機械割":
            if window == 7:  return 95.0, 110.0
            if window == 15: return 95.0, 108.0
            return 96.0, 105.0 # 30d
        elif mode == "G数":
            if window == 7:  return 1500.0, 8000.0
            return 2500.0, 6500.0 # 15/30d
        else: # 差枚
            if window == 7:  return -2000.0, 1000.0
            return -600.0, 800.0 # 15/30d

    daily_stats = {}
    for row in all_data[1:]:
        try:
            d_date, d_store, d_model, d_unit, d_diff, d_games = [c.strip() for c in row]
            if conf['store'] not in d_store: continue
            if d_date not in daily_stats: daily_stats[d_date] = {'all': [], 'A': [], 'B': [], 'C': []}
            entry = {'diff': int(d_diff), 'games': int(d_games)}
            daily_stats[d_date]['all'].append(entry)
            clean_m = d_model.replace("[撤去] ", "")
            if any(clean_m == m for m in conf['A_list']): daily_stats[d_date]['A'].append(entry)
            if any(clean_m == m for m in conf['B_list']): daily_stats[d_date]['B'].append(entry)
            if any(clean_m == m for m in conf['C_list']): daily_stats[d_date]['C'].append(entry)
        except: continue

    sorted_dates = sorted(daily_stats.keys())
    if not sorted_dates: return
    
    base_vals = {'all': [], 'A': [], 'B': [], 'C': []}
    mid_ref = 0 if conf['mode'] == "差枚" else 4000 if conf['mode'] == "G数" else 100

    for d_str in sorted_dates:
        day = daily_stats[d_str]
        for k in base_vals.keys():
            active = [e for e in day[k] if e['games'] > 0]
            if not active: 
                base_vals[k].append(mid_ref)
                continue
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
    if charts:
        doc.batch_update({"requests": [{"deleteEmbeddedObject": {"objectId": c['chartId']}} for c in charts]})

    # 80行目からデータ開始
    header_main = ["日付", "曜日", "店全体", conf['A_name'], conf['B_name'], conf['C_name']]
    for w in windows: header_main += [f"店({w})", f"A({w})", f"B({w})", f"C({w})"]
    
    data_rows = [[d_str, ["月", "火", "水", "木", "金", "土", "日"][datetime.strptime(d_str, "%Y/%m/%d").weekday()]] + 
                 [base_vals[k][i] for k in ['all', 'A', 'B', 'C']] + 
                 [ma_results[w][k][i] for w in windows for k in ['all', 'A', 'B', 'C']] 
                 for i, d_str in enumerate(sorted_dates)]

    # U列以降はグラフ用ソース（80行目から配置）
    ws.update(values=[header_main] + data_rows, range_name='A80')
    ws.update(values=data_rows, range_name='U80')

    s_id = ws.id; l_row = len(data_rows) + 80
    
    def build_panorama_chart(w_idx, window_val, anchor_row):
        min_v, max_v = get_axis_limits(conf['mode'], window_val)
        start_col = 22 + (w_idx * 4) # V, W, X...
        of_cnt = sum(1 for k in ['all','A','B','C'] for v in ma_results[window_val][k] if (v >= max_v or v <= min_v))
        title = f"【{window_val}日MA】灰:全体 赤:{conf['A_name']} 青:{conf['B_name']} 緑:{conf['C_name']} (振切:{of_cnt}回 / 軸:{min_v}〜{max_v})"
        
        series = []
        colors = ["#999999", "#ff0000", "#0000ff", "#00ff00"]
        for i, c in enumerate(range(start_col, start_col + 4)):
            series.append({
                "series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 79, "endRowIndex": l_row, "startColumnIndex": c, "endColumnIndex": c+1}]}},
                "targetAxis": "LEFT_AXIS", "color": hex_to_rgb(colors[i]), "lineStyle": {"width": 2 if i>0 else 4}
            })
        
        return {"addChart": {"chart": {
            "spec": {
                "title": title,
                "basicChart": {
                    "chartType": "LINE", "legendPosition": "BOTTOM_LEGEND",
                    "axis": [
                        {"position": "BOTTOM_AXIS"},
                        {"position": "LEFT_AXIS", "viewWindowOptions": {"viewWindowMin": min_v, "viewWindowMax": max_v, "viewWindowMode": "EXPLICIT"}}
                    ],
                    "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 79, "endRowIndex": l_row, "startColumnIndex": 20, "endColumnIndex": 21}]}}}],
                    "series": series
                }
            },
            "position": {"overlayPosition": {"anchorCell": {"sheetId": s_id, "rowIndex": anchor_row, "columnIndex": 6}, "widthPixels": 5000, "heightPixels": 450}}
        }}}

    # リクエスト一括送信 (列幅調整含む)
    reqs = [
        # A列:100, B列:40, C-F列:85
        {"updateDimensionProperties": {"range": {"sheetId": s_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1}, "properties": {"pixelSize": 100}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": s_id, "dimension": "COLUMNS", "startIndex": 1, "endIndex": 2}, "properties": {"pixelSize": 40}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": s_id, "dimension": "COLUMNS", "startIndex": 2, "endIndex": 6}, "properties": {"pixelSize": 85}, "fields": "pixelSize"}},
        # G列(6)以降を広大なキャンバス(250px)に
        {"updateDimensionProperties": {"range": {"sheetId": s_id, "dimension": "COLUMNS", "startIndex": 6, "endIndex": 30}, "properties": {"pixelSize": 250}, "fields": "pixelSize"}},
        # 表示形式
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 79, "endRowIndex": l_row, "startColumnIndex": 2, "endColumnIndex": 18}, "cell": {"userEnteredFormat": {"numberFormat": get_number_format(conf['mode']), "horizontalAlignment": "CENTER"}}, "fields": "userEnteredFormat"}},
        # グラフ配置
        build_panorama_chart(0, 7, 0), build_panorama_chart(1, 15, 25), build_panorama_chart(2, 30, 50)
    ]
    
    # 色彩ルール (80行目〜)
    for c_idx in range(2, 6):
        reqs.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": s_id, "startRowIndex": 80, "endRowIndex": l_row, "startColumnIndex": c_idx, "endColumnIndex": c_idx+1}],
            "booleanRule": {"condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": str(mid_ref)}]}, "format": {"backgroundColor": hex_to_rgb("#cfe2f3"), "textFormat": {"foregroundColor": hex_to_rgb("#0000ff")}}}}, "index": 0}})
        reqs.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": s_id, "startRowIndex": 80, "endRowIndex": l_row, "startColumnIndex": c_idx, "endColumnIndex": c_idx+1}],
            "booleanRule": {"condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": str(mid_ref)}]}, "format": {"backgroundColor": hex_to_rgb("#f4cccc"), "textFormat": {"foregroundColor": hex_to_rgb("#ff0000")}}}}, "index": 0}})

    doc.batch_update({"requests": reqs})
    print("   -> 完了。G列以降に広大なパノラマグラフを、80行目以降にデータを配置しました。")

# ==========================================
# BLOCK: 4. 機種別分析（安定動作版）
# ==========================================
async def execute_single_analysis(doc, conf, all_data):
    if not conf['target_model']: return
    print(f"   > 機種別分析: {conf['target_model']}")
    model_data = {}; unique_units = set()
    for row in all_data[1:]:
        try:
            d_date, d_store, d_model, d_unit, d_diff, d_games = [c.strip() for c in row]
            if conf['store'] in d_store and conf['target_model'] in d_model:
                unique_units.add(d_unit); 
                if d_date not in model_data: model_data[d_date] = {}
                model_data[d_date][d_unit] = {'diff': int(d_diff), 'games': int(d_games)}
        except: continue
    if not unique_units: return
    sorted_units = sorted(list(unique_units), key=lambda x: int(x) if x.isdigit() else x)
    sorted_dates = sorted(model_data.keys())
    header = ["日付", "曜日", "機種総差枚", "台平均差枚", "平均G", "機械割", "粘り勝率"] + [f"{u}#" for u in sorted_units]
    data_rows = [[d_str, "", sum(u['diff'] for u in model_data[d_str].values()), sum(u['diff'] for u in model_data[d_str].values())/len(model_data[d_str]), 
                 sum(u['games'] for u in model_data[d_str].values())/len(model_data[d_str]), 
                 ((sum(u['games'] for u in model_data[d_str].values())*3+sum(u['diff'] for u in model_data[d_str].values()))/(sum(u['games'] for u in model_data[d_str].values())*3)*100) if sum(u['games'] for u in model_data[d_str].values())>0 else 0,
                 len([u for u in model_data[d_str].values() if u['games']>=5000 and u['diff']>0])/len(model_data[d_str])] + [model_data[d_str][u]['diff'] if u in model_data[d_str] else "" for u in sorted_units] for d_str in sorted_dates]
    
    ws = doc.worksheet(SINGLE_SHEET); ws.clear(); ws.update(values=[header] + data_rows, range_name='A2')
    s_id = ws.id; l_row, l_col = len(data_rows) + 2, len(header)
    reqs = [
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 2, "endRowIndex": l_row, "startColumnIndex": 2, "endColumnIndex": 4}, "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0\"枚\""}}}, "fields": "userEnteredFormat"}},
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 2, "endRowIndex": l_row, "startColumnIndex": 4, "endColumnIndex": 5}, "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0\"G\""}}}, "fields": "userEnteredFormat"}},
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 2, "endRowIndex": l_row, "startColumnIndex": 5, "endColumnIndex": 6}, "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "0.00\"%\""}}}, "fields": "userEnteredFormat"}},
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 2, "endRowIndex": l_row, "startColumnIndex": 6, "endColumnIndex": 7}, "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}}, "fields": "userEnteredFormat"}}
    ]
    t_range = [{"sheetId": s_id, "startRowIndex": 2, "endRowIndex": l_row, "startColumnIndex": 7, "endColumnIndex": l_col}]
    def add_cf(formula, bg, text, bold=False):
        return {"addConditionalFormatRule": {"rule": {"ranges": t_range, "booleanRule": {"condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": formula}]}, 
                "format": {"backgroundColor": hex_to_rgb(bg), "textFormat": {"foregroundColor": hex_to_rgb(text), "bold": bold}}}}, "index": 0}}
    reqs += [
        add_cf("=H3<=-3001","#f4cccc","#ff0000",True), add_cf("=AND(H3>=-3000,H3<=-1500)","#fff2cc","#ff0000",True), add_cf("=AND(H3>=-1499,H3<=-1)","#fff2cc","#ff0000",False),
        add_cf("=AND(H3>=0,H3<=2000)","#fff2cc","#000000",False), add_cf("=AND(H3>=2001,H3<=3499)","#cfe2f3","#000000",False), add_cf("=AND(H3>=3500,H3<=4999)","#9fc5e8","#000000",False),
        add_cf("=AND(H3>=5000,H3<=7999)","#6fa8dc","#ffffff",True), add_cf("=AND(H3>=8000,H3<=11999)","#3d85c6","#ffffff",True), add_cf("=H3>=12000","#0b5394","#ffffff",True)
    ]
    doc.batch_update({"requests": reqs})
    print("   -> 機種別分析 完了")

# ==========================================
# BLOCK: 5. メインループ
# ==========================================
async def main():
    print(f"\n--- Ver.3.9 起動 (UI Master Edition) [{datetime.now().strftime('%H:%M:%S')}] ---")
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
    gc = gspread.authorize(creds); doc = gc.open_by_key(SPREADSHEET_KEY); conf_ws = doc.worksheet(CONFIG_SHEET)
    
    # 起動時に即同期
    await sync_store_list(doc)

    while True:
        try:
            conf_ws.update_acell('D2', f"監視中: {datetime.now().strftime('%H:%M:%S')}")
            vals = conf_ws.get_all_values()
            all_cmd, single_cmd, cross_cmd = vals[1][1], vals[7][2], vals[9][2]
            
            if "実行" in str([all_cmd, single_cmd, cross_cmd]):
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 命令受信。")
                all_data = await sync_store_list(doc) # 実行時も一応同期
                vals = conf_ws.get_all_values()
                conf = {
                    "store": vals[4][1], "mode": vals[9][1], "target_model": vals[7][1],
                    "A_name": vals[10][1], "A_list": [v[1] for v in vals[11:23] if v[1]],
                    "B_name": vals[23][1], "B_list": [v[1] for v in vals[24:36] if v[1]],
                    "C_name": vals[36][1], "C_list": [v[1] for v in vals[37:49] if v[1]]
                }
                btn_cell = 'B2' if "実行" in all_cmd else ('C8' if "実行" in single_cmd else 'C10')
                conf_ws.update_acell(btn_cell, "● 実行中")
                if "実行" in all_cmd or "実行" in cross_cmd: await execute_cross_analysis(doc, conf, all_data)
                if "実行" in all_cmd or "実行" in single_cmd: await execute_single_analysis(doc, conf, all_data)
                conf_ws.update_acell(btn_cell, "待機中"); print("   > 完了。")
        except Exception as e: print(f"Error: {e}")
        await asyncio.sleep(15)

if __name__ == "__main__": asyncio.run(main())