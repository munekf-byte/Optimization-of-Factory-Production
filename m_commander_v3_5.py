# --- VERSION: m_commander_v3_5_Stable_Repair_20260201 ---
# 修正内容: B10指標の座標修正、シート削除回避、数値形式(NumberFormat)の適正化、リミッターカウンター実装

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
LOCAL_DATABASE  = "minrepo_database.csv"

# ==========================================
# BLOCK: 2. 道具箱
# ==========================================
def hex_to_rgb(hex_str):
    hex_str = hex_str.lstrip('#')
    return {"red": int(hex_str[0:2], 16)/255.0, "green": int(hex_str[2:4], 16)/255.0, "blue": int(hex_str[4:6], 16)/255.0}

def get_number_format(mode):
    if mode == "機械割": return {"type": "NUMBER", "pattern": "0.00\"%\""}
    if mode == "G数": return {"type": "NUMBER", "pattern": "#,##0\"G\""}
    return {"type": "NUMBER", "pattern": "#,##0\"枚\""}

# ==========================================
# BLOCK: 3. クロス分析エンジン（リミッター＆カウンター）
# ==========================================
async def execute_cross_analysis(doc, conf, all_data):
    print("   > クロス分析エンジン始動...")
    
    # 指標別の軸設定（リーダー指定）
    if conf['mode'] == "機械割": 
        min_v, max_v, midpoint = 95, 110, 100
    elif conf['mode'] == "G数":
        min_v, max_v, midpoint = 1500, 6500, 4000
    else: # 差枚
        min_v, max_v, midpoint = -600, 600, 0

    daily_stats = {}
    for row in all_data[1:]:
        try:
            d_date, d_store, d_model, d_unit, d_diff, d_games = [c.strip() for c in row]
            if conf['store'] not in d_store: continue
            entry = {'diff': int(d_diff), 'games': int(d_games)}
            if d_date not in daily_stats: daily_stats[d_date] = {'all': [], 'A': [], 'B': [], 'C': []}
            daily_stats[d_date]['all'].append(entry)
            clean_m = d_model.replace("[撤去] ", "")
            if any(clean_m == m for m in conf['A_list']): daily_stats[d_date]['A'].append(entry)
            if any(clean_m == m for m in conf['B_list']): daily_stats[d_date]['B'].append(entry)
            if any(clean_m == m for m in conf['C_list']): daily_stats[d_date]['C'].append(entry)
        except: continue

    sorted_dates = sorted(daily_stats.keys())
    base_vals = {'all': [], 'A': [], 'B': [], 'C': []}
    overflow_count = 0 

    for d_str in sorted_dates:
        day = daily_stats[d_str]
        for k in base_vals.keys():
            active = [e for e in day[k] if e['games'] > 0]
            if not active: 
                base_vals[k].append(midpoint)
                continue
            
            if conf['mode'] == "差枚": val = sum(e['diff'] for e in active) / len(active)
            elif conf['mode'] == "G数": val = sum(e['games'] for e in active) / len(active)
            else: # 機械割
                t_d, t_g = sum(e['diff'] for e in active), sum(e['games'] for e in active)
                val = ((t_g * 3 + t_d) / (t_g * 3) * 100) if t_g > 0 else 100
            
            if val < min_v or val > max_v: overflow_count += 1
            base_vals[k].append(round(val, 2))

    # 移動平均の計算
    ma_results = {w: {k: [round(v, 2) for v in (lambda data, win: [sum(data[max(0, i-win+1):i+1])/len(data[max(0, i-win+1):i+1]) for i in range(len(data))])(base_vals[k], w)] for k in base_vals.keys()} for w in [3, 7, 15]}

    ws = doc.worksheet(CROSS_SHEET)
    ws.clear()
    
    header_info = [["指標", conf['mode'], "軸範囲", f"{min_v}〜{max_v}", "リミッター作動数", f"{overflow_count}回"]]
    header_main = ["日付", "曜日", "店全体", conf['A_name'], conf['B_name'], conf['C_name']]
    for w in [3, 7, 15]: header_main += [f"店({w})", f"A({w})", f"B({w})", f"C({w})"]
    
    data_rows = []
    chart_data_rows = [] # グラフ用（リミッター加工済み）
    for i, d_str in enumerate(sorted_dates):
        dow = ["月", "火", "水", "木", "金", "土", "日"][datetime.strptime(d_str, "%Y/%m/%d").weekday()]
        row = [d_str, dow] + [base_vals[k][i] for k in ['all', 'A', 'B', 'C']] + [ma_results[w][k][i] for w in [3, 7, 15] for k in ['all', 'A', 'B', 'C']]
        data_rows.append(row)
        # グラフ用データ作成（U列以降に配置）
        c_row = [max(min_v, min(max_v, v)) if isinstance(v, (int, float)) else v for v in row]
        chart_data_rows.append(c_row)

    ws.update(values=header_info, range_name='A1')
    ws.update(values=[header_main] + data_rows, range_name='A50')
    ws.update(values=chart_data_rows, range_name='U50')

    s_id = ws.id
    l_row = len(data_rows) + 50
    
    def build_chart_req(title, start_col, anchor_row):
        colors = ["#999999", "#ff0000", "#0000ff", "#00ff00"]
        series = []
        for i, c in enumerate(range(start_col, start_col + 4)):
            series.append({"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 49, "endRowIndex": l_row, "startColumnIndex": c, "endColumnIndex": c+1}]}},
                           "targetAxis": "LEFT_AXIS", "color": hex_to_rgb(colors[i]), "lineStyle": {"width": 2 if i>0 else 5}})
        return { "addChart": { "chart": { "spec": { "title": title, "basicChart": { "chartType": "LINE", "legendPosition": "BOTTOM_LEGEND",
                "axis": [{"position": "BOTTOM_AXIS"}, {"position": "LEFT_AXIS", "viewWindowOptions": {"viewWindowMin": min_v, "viewWindowMax": max_v, "viewWindowMode": "EXPLICIT"}}],
                "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 49, "endRowIndex": l_row, "startColumnIndex": 20, "endColumnIndex": 21}]}}}], "series": series } },
                "position": { "overlayPosition": { "anchorCell": { "sheetId": s_id, "rowIndex": anchor_row, "columnIndex": 0 }, "widthPixels": 1000, "heightPixels": 380 } } } } }

    reqs = [
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 49, "endRowIndex": l_row, "startColumnIndex": 2, "endColumnIndex": 18}, 
                        "cell": {"userEnteredFormat": {"numberFormat": get_number_format(conf['mode']), "horizontalAlignment": "CENTER"}}, "fields": "userEnteredFormat"}},
        {"updateDimensionProperties": {"range": {"sheetId": s_id, "dimension": "COLUMNS", "startIndex": 20, "endIndex": 40}, "properties": {"pixelSize": 2}, "fields": "pixelSize"}}, 
        build_chart_req(f"【3日トレンド】リミッター適用済 (振切:{overflow_count}回)", 22, 0),
        build_chart_req(f"7日トレンド", 26, 16),
        build_chart_req(f"15日トレンド", 30, 32)
    ]
    
    # 色彩ルール（店全体〜部門Cまで）
    for c_idx in range(2, 6):
        reqs.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": s_id, "startRowIndex": 50, "endRowIndex": l_row, "startColumnIndex": c_idx, "endColumnIndex": c_idx+1}],
            "booleanRule": {"condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": str(midpoint)}]}, "format": {"backgroundColor": hex_to_rgb("#cfe2f3"), "textFormat": {"foregroundColor": hex_to_rgb("#0000ff")}}}}, "index": 0}})
        reqs.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": s_id, "startRowIndex": 50, "endRowIndex": l_row, "startColumnIndex": c_idx, "endColumnIndex": c_idx+1}],
            "booleanRule": {"condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": str(midpoint)}]}, "format": {"backgroundColor": hex_to_rgb("#f4cccc"), "textFormat": {"foregroundColor": hex_to_rgb("#ff0000")}}}}, "index": 0}})

    doc.batch_update({"requests": reqs})
    print("   -> クロス分析 完了")

# ==========================================
# BLOCK: 4. 単独機種分析（表示形式＆9段階色彩）
# ==========================================
async def execute_single_analysis(doc, conf, all_data):
    if not conf['target_model']: return
    print(f"   > 単独機種分析: {conf['target_model']}")
    
    model_data = {}
    unique_units = set()
    for row in all_data[1:]:
        try:
            d_date, d_store, d_model, d_unit, d_diff, d_games = [c.strip() for c in row]
            if conf['store'] in d_store and conf['target_model'] in d_model:
                unique_units.add(d_unit)
                if d_date not in model_data: model_data[d_date] = {}
                model_data[d_date][d_unit] = {'diff': int(d_diff), 'games': int(d_games)}
        except: continue

    if not unique_units: return
    sorted_units = sorted(list(unique_units), key=lambda x: int(x) if x.isdigit() else x)
    sorted_dates = sorted(model_data.keys())

    header = ["日付", "曜日", "機種総差枚", "台平均差枚", "平均G", "機械割", "粘り勝率"] + [f"{u}#" for u in sorted_units]
    data_rows = []
    for d_str in sorted_dates:
        day = model_data[d_str]
        t_d, t_g = sum(u['diff'] for u in day.values()), sum(u['games'] for u in day.values())
        avg_d, avg_g = t_d/len(day), t_g/len(day)
        m_rate = ((t_g * 3 + t_d) / (t_g * 3)) * 100 if t_g > 0 else 0
        sticky = len([u for u in day.values() if u['games']>=5000 and u['diff']>0])/len(day)
        data_rows.append([d_str, "", t_d, avg_d, avg_g, m_rate, sticky] + [day[u]['diff'] if u in day else "" for u in sorted_units])

    ws = doc.worksheet(SINGLE_SHEET)
    ws.clear()
    ws.update(values=[header] + data_rows, range_name='A2')
    
    s_id = ws.id
    l_row, l_col = len(data_rows) + 2, len(header)
    
    # 数値形式の一括設定（APIバッチ）
    reqs = [
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 2, "endRowIndex": l_row, "startColumnIndex": 2, "endColumnIndex": 4}, "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0\"枚\""}}}, "fields": "userEnteredFormat"}},
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 2, "endRowIndex": l_row, "startColumnIndex": 4, "endColumnIndex": 5}, "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0\"G\""}}}, "fields": "userEnteredFormat"}},
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 2, "endRowIndex": l_row, "startColumnIndex": 5, "endColumnIndex": 6}, "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "0.00\"%\""}}}, "fields": "userEnteredFormat"}},
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 2, "endRowIndex": l_row, "startColumnIndex": 6, "endColumnIndex": 7}, "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}}, "fields": "userEnteredFormat"}}
    ]
    
    # 9段階色彩ルール（CUSTOM_FORMULAを数値参照で適用）
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
    print("   -> 単独分析 完了")

# ==========================================
# BLOCK: 5. 司令部（監視メイン）
# ==========================================
async def main():
    print(f"\n--- Ver.3.5 起動 (安定板) [{datetime.now().strftime('%H:%M:%S')}] ---")
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    gc = gspread.authorize(creds)
    doc = gc.open_by_key(SPREADSHEET_KEY)
    conf_ws = doc.worksheet(CONFIG_SHEET)

    while True:
        try:
            # 監視状況の打刻（GAS側の座標に合わせる）
            conf_ws.update_acell('D2', f"監視中: {datetime.now().strftime('%H:%M:%S')}")
            vals = conf_ws.get_all_values()
            
            # 実行ボタン座標: B2, C8, C10
            all_cmd, single_cmd, cross_cmd = vals[1][1], vals[7][2], vals[9][2]
            
            if "実行" in str([all_cmd, single_cmd, cross_cmd]):
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 命令を受信しました。")
                
                # CSV読み込み
                with open(LOCAL_DATABASE, mode='r', encoding='utf-8-sig') as f:
                    all_data = list(csv.reader(f))

                # コンフィグ抽出（GASの座標に完全同期）
                conf = {
                    "store": vals[4][1], 
                    "mode": vals[9][1], # B10
                    "target_model": vals[7][1], # B8
                    "A_name": vals[10][1], "A_list": [v[1] for v in vals[11:23] if v[1]],
                    "B_name": vals[23][1], "B_list": [v[1] for v in vals[24:36] if v[1]],
                    "C_name": vals[36][1], "C_list": [v[1] for v in vals[37:49] if v[1]]
                }
                
                # ボタン表示を「実行中」へ
                btn_cell = 'B2' if "実行" in all_cmd else ('C8' if "実行" in single_cmd else 'C10')
                conf_ws.update_acell(btn_cell, "● 実行中")

                # 各分析実行
                if "実行" in all_cmd or "実行" in cross_cmd: await execute_cross_analysis(doc, conf, all_data)
                if "実行" in all_cmd or "実行" in single_cmd: await execute_single_analysis(doc, conf, all_data)
                
                # 完了後の復帰
                conf_ws.update_acell(btn_cell, "待機中")
                print("   > 処理完了。")

        except Exception as e:
            print(f"システムエラー: {e}")
        await asyncio.sleep(15)

if __name__ == "__main__":
    asyncio.run(main())