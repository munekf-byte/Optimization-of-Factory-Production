# --- VERSION: m_analyzer_v2.12_Strategic_Command_20260129 ---

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import asyncio
import re

# ==========================================
# BLOCK: 1. 固定設定
# ==========================================
SPREADSHEET_KEY = "1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I"
RAW_DATA_SHEET  = "生データ"
CONFIG_SHEET    = "分析設定"
CROSS_SHEET     = "クロス分析"
INDEX_SHEET     = "機種目録"

# ==========================================
# BLOCK: 2. 道具箱
# ==========================================
def hex_to_rgb(hex_str):
    hex_str = hex_str.lstrip('#')
    return {"red": int(hex_str[0:2], 16)/255.0, "green": int(hex_str[2:4], 16)/255.0, "blue": int(hex_str[4:6], 16)/255.0}

def calculate_machine_rate(total_diff, total_games):
    if total_games == 0: return 0
    return round(((total_games * 3) + total_diff) / (total_games * 3) * 100, 2)

def calculate_ma(data_list, window):
    ma_list = []
    for i in range(len(data_list)):
        start = max(0, i - window + 1)
        subset = data_list[start:i+1]
        avg = sum(subset) / len(subset)
        ma_list.append(round(avg, 2))
    return ma_list

# ==========================================
# BLOCK: 3. 機種目録エンジン（実稼働G対応）
# ==========================================
def update_model_index_v2(doc, all_data):
    print("--- 1. 機種目録を更新中... ---")
    all_dates = [datetime.strptime(r[0], "%Y/%m/%d") for r in all_data[1:] if r[0]]
    max_date = max(all_dates) if all_dates else datetime.now()
    active_threshold = max_date - timedelta(days=7)

    model_stats = {}
    for row in all_data[1:]:
        try:
            d_date_str, d_model, d_games = row[0], row[2], int(row[5])
            if d_model not in model_stats:
                model_stats[d_model] = {'store': row[1], 'last_dt': datetime.strptime(d_date_str, "%Y/%m/%d"), 'sum_g': 0, 'active_days_count': 0, 'units': set()}
            if d_games > 0:
                model_stats[d_model]['sum_g'] += d_games
                model_stats[d_model]['active_days_count'] += 1
            model_stats[d_model]['units'].add(row[3])
            dt = datetime.strptime(d_date_str, "%Y/%m/%d")
            if dt > model_stats[d_model]['last_dt']: model_stats[d_model]['last_dt'] = dt
        except: continue

    active_list, withdrawn_list = [], []
    for name, s in model_stats.items():
        avg_g = int(s['sum_g'] / s['active_days_count']) if s['active_days_count'] > 0 else 0
        info = [s['store'], name, len(s['units']), s['last_dt'].strftime("%Y/%m/%d"), avg_g]
        if s['last_dt'] >= active_threshold: active_list.append(info)
        else: info[1] = f"[撤去] {info[1]}"; withdrawn_list.append(info)

    active_list.sort(key=lambda x: x[4], reverse=True)
    index_ws = doc.worksheet(INDEX_SHEET)
    index_ws.clear()
    index_ws.update(values=[["店舗名", "機種名", "設置台数", "最終稼働日", "平均稼働G"]] + active_list + withdrawn_list, range_name='A1')

# ==========================================
# BLOCK: 4. 分析設定読み込み
# ==========================================
def get_config_v3(doc):
    conf_ws = doc.worksheet(CONFIG_SHEET)
    vals = conf_ws.get_all_values()
    return {
        "store": vals[1][1], "mode": vals[2][1],
        "A_name": vals[3][1] or "部門A", "A_list": [v[1] for v in vals[4:14] if v[1]],
        "B_name": vals[14][1] or "部門B", "B_list": [v[1] for v in vals[15:25] if v[1]],
        "C_name": vals[25][1] or "部門C", "C_list": [v[1] for v in vals[26:36] if v[1]]
    }

# ==========================================
# BLOCK: 5. メインエンジン（巨大3段グラフ）
# ==========================================
async def run_analysis():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    gc = gspread.authorize(creds)
    doc = gc.open_by_key(SPREADSHEET_KEY)
    
    all_data = doc.worksheet(RAW_DATA_SHEET).get_all_values()
    update_model_index_v2(doc, all_data)
    conf = get_config_v3(doc)

    print(f"--- 2. クロス分析を実行中... ({conf['mode']}) ---")
    daily_stats = {}
    for row in all_data[1:]:
        try:
            d_date, d_store, d_model = row[0], row[1], row[2]
            if conf['store'] not in d_store: continue
            entry = {'diff': int(row[4]), 'games': int(row[5])}
            if d_date not in daily_stats: daily_stats[d_date] = {'all': [], 'A': [], 'B': [], 'C': []}
            daily_stats[d_date]['all'].append(entry)
            clean_m = d_model.replace("[撤去] ", "")
            if any(clean_m == m for m in conf['A_list']): daily_stats[d_date]['A'].append(entry)
            if any(clean_m == m for m in conf['B_list']): daily_stats[d_date]['B'].append(entry)
            if any(clean_m == m for m in conf['C_list']): daily_stats[d_date]['C'].append(entry)
        except: continue

    sorted_dates = sorted(daily_stats.keys(), reverse=False)
    base_vals = {'all': [], 'A': [], 'B': [], 'C': []}
    for d_str in sorted_dates:
        day = daily_stats[d_str]
        def get_v(entries):
            active_entries = [e for e in entries if e['games'] > 0]
            if not active_entries: return 0
            if conf['mode'] == "差枚": return int(sum(e['diff'] for e in active_entries) / len(active_entries))
            if conf['mode'] == "G数": return int(sum(e['games'] for e in active_entries) / len(active_entries))
            return calculate_machine_rate(sum(e['diff'] for e in active_entries), sum(e['games'] for e in active_entries))
        for k in base_vals.keys(): base_vals[k].append(get_v(day[k]))

    # MA算出
    ma_results = {w: {k: calculate_ma(v, w) for k, v in base_vals.items()} for w in [3, 7, 15]}

    # 表データ構築
    unit = "枚" if conf['mode'] == "差枚" else "G" if conf['mode'] == "G数" else "%"
    header = ["日付", "曜日", "店全体", conf['A_name'], conf['B_name'], conf['C_name']]
    for w in [3, 7, 15]: header += [f"店MA({w})", f"{conf['A_name']}({w})", f"{conf['B_name']}({w})", f"{conf['C_name']}({w})"]
    
    data_rows = []
    for i, d_str in enumerate(sorted_dates):
        dow = ["月", "火", "水", "木", "金", "土", "日"][datetime.strptime(d_str, "%Y/%m/%d").weekday()]
        row = [d_str, dow]
        # C〜F列（単位付き）
        row += [f"{base_vals[k][i]}{unit}" for k in ['all', 'A', 'B', 'C']]
        # G〜V列（計算用数値）
        for w in [3, 7, 15]: row += [ma_results[w][k][i] for k in ['all', 'A', 'B', 'C']]
        data_rows.append(row)

    def f_avg(lst): 
        res = round(sum(lst)/len(lst), 2) if lst else 0
        return f"{res}%" if conf['mode'] == "機械割" else f"{int(res)}{unit}"

    avg_row = ["総合平均", "-", f_avg(base_vals['all']), f_avg(base_vals['A']), f_avg(base_vals['B']), f_avg(base_vals['C'])] + [""] * 12
    
    try:
        cross_ws = doc.worksheet(CROSS_SHEET)
        doc.del_worksheet(cross_ws)
    except: pass
    cross_ws = doc.add_worksheet(title=CROSS_SHEET, rows=len(data_rows)+50, cols=25)
    cross_ws.update(values=[avg_row, header] + data_rows, range_name='A1')

    # --- 視覚化魔法：バッチ更新 ---
    print("--- 3. 巨大3段グラフと書式を適用中... ---")
    s_id = cross_ws.id
    l_row, l_col = len(data_rows) + 2, len(header)
    midpoint = 100 if conf['mode'] == "機械割" else 0
    
    requests = [
        {"repeatCell": {"range": {"sheetId": s_id}, "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE", "wrapStrategy": "WRAP"}}, "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,wrapStrategy)"}},
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 0, "endRowIndex": 1}, "cell": {"userEnteredFormat": {"backgroundColor": hex_to_rgb("#fff2cc"), "textFormat": {"bold": True}}}, "fields": "userEnteredFormat(backgroundColor,textFormat.bold)"}},
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 1, "endRowIndex": 2}, "cell": {"userEnteredFormat": {"backgroundColor": hex_to_rgb("#cccccc"), "textFormat": {"bold": True}}}, "fields": "userEnteredFormat(backgroundColor,textFormat.bold)"}},
        {"updateDimensionProperties": {"range": {"sheetId": s_id, "dimension": "COLUMNS", "startIndex": 6, "endIndex": 18}, "properties": {"pixelSize": 20}, "fields": "pixelSize"}} # MA列を極細に
    ]

    # 色彩ルール
    for c_idx in range(2, 6):
        requests.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": s_id, "startRowIndex": 2, "endRowIndex": l_row, "startColumnIndex": c_idx, "endColumnIndex": c_idx+1}],
            "booleanRule": {"condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": str(midpoint)}]}, "format": {"backgroundColor": hex_to_rgb("#cfe2f3"), "textFormat": {"foregroundColor": hex_to_rgb("#0000ff")}}}}, "index": 0}})
        requests.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": s_id, "startRowIndex": 2, "endRowIndex": l_row, "startColumnIndex": c_idx, "endColumnIndex": c_idx+1}],
            "booleanRule": {"condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": str(midpoint)}]}, "format": {"backgroundColor": hex_to_rgb("#f4cccc"), "textFormat": {"foregroundColor": hex_to_rgb("#ff0000")}}}}, "index": 0}})

    # グラフ生成関数
    def build_chart(title, start_col, anchor_row):
        colors = ["#cccccc", "#ff0000", "#0000ff", "#00ff00"]
        widths = [6, 2, 2, 2] # 店全体を極太に
        series = []
        for i, c in enumerate(range(start_col, start_col + 4)):
            series.append({"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 1, "endRowIndex": l_row, "startColumnIndex": c, "endColumnIndex": c+1}]}},
                           "targetAxis": "LEFT_AXIS", "color": hex_to_rgb(colors[i]), "lineStyle": {"width": widths[i]}})
        
        min_v, max_v = (95, 105) if conf['mode'] == "機械割" else (-800, 800)
        return { "addChart": { "chart": { "spec": { "title": title, "basicChart": { "chartType": "LINE", "legendPosition": "BOTTOM_LEGEND",
                "axis": [{"position": "BOTTOM_AXIS", "title": "時系列"}, {"position": "LEFT_AXIS", "title": conf['mode'], "viewWindowOptions": {"viewWindowMin": min_v, "viewWindowMax": max_v, "viewWindowMode": "EXPLICIT"}}],
                "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 1, "endRowIndex": l_row, "startColumnIndex": 0, "endColumnIndex": 1}]}}}], "series": series } },
                "position": { "overlayPosition": { "anchorCell": { "sheetId": s_id, "rowIndex": anchor_row, "columnIndex": 6 }, "widthPixels": 900, "heightPixels": 450 } } } } }

    requests.append(build_chart(f"【{conf['mode']}】 3日移動平均 (直近の勢い)", 6, 0))
    requests.append(build_chart(f"【{conf['mode']}】 7日移動平均 (週次トレンド)", 10, 24))
    requests.append(build_chart(f"【{conf['mode']}】 15日移動平均 (長期戦略)", 14, 48))

    doc.batch_update({"requests": requests})
    cross_ws.freeze(rows=2, cols=2)
    print("\n【完遂】3段巨大チャート ＋ 実稼働平均 ＋ 命名反映が完了しました！")

if __name__ == "__main__":
    asyncio.run(run_analysis())