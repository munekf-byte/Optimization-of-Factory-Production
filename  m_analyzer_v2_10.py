# --- VERSION: m_analyzer_v2.10_Dual_Stream_Edition_20260129 ---

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
# BLOCK: 3. 機種目録エンジン
# ==========================================
def update_model_index_v2(doc, all_data):
    print("--- 1. 機種目録を更新中... ---")
    all_dates = [datetime.strptime(r[0], "%Y/%m/%d") for r in all_data[1:] if r[0]]
    max_date = max(all_dates) if all_dates else datetime.now()
    active_threshold = max_date - timedelta(days=7)

    model_stats = {}
    for row in all_data[1:]:
        try:
            d_date_str, d_model, d_games = row[0], row[2], row[5]
            dt = datetime.strptime(d_date_str, "%Y/%m/%d")
            if d_model not in model_stats:
                model_stats[d_model] = {'store': row[1], 'last_dt': dt, 'sum_g': 0, 'days': set(), 'units': set()}
            model_stats[d_model]['sum_g'] += int(d_games)
            model_stats[d_model]['days'].add(d_date_str)
            model_stats[d_model]['units'].add(row[3])
            if dt > model_stats[d_model]['last_dt']: model_stats[d_model]['last_dt'] = dt
        except: continue

    active_list, withdrawn_list = [], []
    for name, s in model_stats.items():
        avg_g = int(s['sum_g'] / len(s['days'])) if s['days'] else 0
        info = [s['store'], name, len(s['units']), s['last_dt'].strftime("%Y/%m/%d"), avg_g]
        if s['last_dt'] >= active_threshold: active_list.append(info)
        else: info[1] = f"[撤去] {info[1]}"; withdrawn_list.append(info)

    active_list.sort(key=lambda x: x[4], reverse=True)
    index_rows = [["店舗名", "機種名", "設置台数", "最終稼働日", "平均稼働G"]] + active_list + withdrawn_list
    index_ws = doc.worksheet(INDEX_SHEET)
    index_ws.clear()
    index_ws.update(values=index_rows, range_name='A1')

# ==========================================
# BLOCK: 4. メイン分析エンジン
# ==========================================
async def run_analysis():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    gc = gspread.authorize(creds)
    doc = gc.open_by_key(SPREADSHEET_KEY)
    
    raw_ws = doc.worksheet(RAW_DATA_SHEET)
    all_data = raw_ws.get_all_values()
    update_model_index_v2(doc, all_data)
    
    conf_ws = doc.worksheet(CONFIG_SHEET)
    c_vals = conf_ws.get_all_values()
    conf = {"store": c_vals[1][1], "mode": c_vals[2][1], "A": [v[1] for v in c_vals[4:14] if v[1]], "B": [v[1] for v in c_vals[15:25] if v[1]], "C": [v[1] for v in c_vals[26:36] if v[1]]}

    print(f"--- 2. トレンド分析(MA)を実行中... ({conf['mode']}) ---")
    daily_stats = {}
    for row in all_data[1:]:
        try:
            d_date, d_store, d_model = row[0], row[1], row[2]
            if conf['store'] not in d_store: continue
            entry = {'diff': int(row[4]), 'games': int(row[5])}
            if d_date not in daily_stats: daily_stats[d_date] = {'all': [], 'A': [], 'B': [], 'C': []}
            daily_stats[d_date]['all'].append(entry)
            if any(m in d_model for m in conf['A']): daily_stats[d_date]['A'].append(entry)
            if any(m in d_model for m in conf['B']): daily_stats[d_date]['B'].append(entry)
            if any(m in d_model for m in conf['C']): daily_stats[d_date]['C'].append(entry)
        except: continue

    sorted_dates = sorted(daily_stats.keys(), reverse=False)
    base_vals = {'all': [], 'A': [], 'B': [], 'C': []}
    for d_str in sorted_dates:
        day = daily_stats[d_str]
        def get_v(entries):
            if not entries: return 0
            if conf['mode'] == "差枚": return int(sum(e['diff'] for e in entries) / len(entries))
            return calculate_machine_rate(sum(e['diff'] for e in entries), sum(e['games'] for e in entries))
        for k in base_vals.keys(): base_vals[k].append(get_v(day[k]))

    ma7 = {k: calculate_ma(v, 7) for k, v in base_vals.items()}
    ma15 = {k: calculate_ma(v, 15) for k, v in base_vals.items()}

    header = ["日付", "曜日", "店全体", "部門A", "部門B", "部門C", "店MA(7)", "A-MA(7)", "B-MA(7)", "C-MA(7)", "店MA(15)", "A-MA(15)", "B-MA(15)", "C-MA(15)"]
    data_rows = []
    for i, d_str in enumerate(sorted_dates):
        dow = ["月", "火", "水", "木", "金", "土", "日"][datetime.strptime(d_str, "%Y/%m/%d").weekday()]
        row = [d_str, dow, base_vals['all'][i], base_vals['A'][i], base_vals['B'][i], base_vals['C'][i]]
        row += [ma7['all'][i], ma7['A'][i], ma7['B'][i], ma7['C'][i]]
        row += [ma15['all'][i], ma15['A'][i], ma15['B'][i], ma15['C'][i]]
        data_rows.append(row)

    def get_f_avg(lst):
        if not lst: return "-"
        avg = round(sum(lst)/len(lst), 2)
        return f"{avg}%" if conf['mode'] == "機械割" else int(avg)

    avg_row = ["総合平均", "-", get_f_avg(base_vals['all']), get_f_avg(base_vals['A']), get_f_avg(base_vals['B']), get_f_avg(base_vals['C'])]
    final_rows = [avg_row, header] + data_rows

    try:
        cross_ws = doc.worksheet(CROSS_SHEET)
        doc.del_worksheet(cross_ws)
    except: pass
    cross_ws = doc.add_worksheet(title=CROSS_SHEET, rows=len(final_rows)+50, cols=20)
    cross_ws.update(values=final_rows, range_name='A1')

    # ==========================================
    # BLOCK: 5. 視覚化魔法（二連グラフ実装）
    # ==========================================
    s_id = cross_ws.id
    l_row, l_col = len(final_rows), len(header)
    
    print("--- 3. グラフと色彩を適用中... ---")
    midpoint = 100 if conf['mode'] == "機械割" else 0
    base_reqs = [
        {"repeatCell": {"range": {"sheetId": s_id}, "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE", "textFormat": {"fontSize": 10}}}, "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat.fontSize)"}},
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 0, "endRowIndex": 1}, "cell": {"userEnteredFormat": {"backgroundColor": hex_to_rgb("#fff2cc"), "textFormat": {"bold": True}}}, "fields": "userEnteredFormat(backgroundColor,textFormat.bold)"}},
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 1, "endRowIndex": 2}, "cell": {"userEnteredFormat": {"backgroundColor": hex_to_rgb("#cccccc"), "textFormat": {"bold": True}}}, "fields": "userEnteredFormat(backgroundColor,textFormat.bold)"}}
    ]
    # 色彩ルール
    for c_idx in range(2, l_col):
        base_reqs.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": s_id, "startRowIndex": 2, "endRowIndex": l_row, "startColumnIndex": c_idx, "endColumnIndex": c_idx+1}],
            "booleanRule": {"condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": str(midpoint)}]}, "format": {"backgroundColor": hex_to_rgb("#cfe2f3"), "textFormat": {"foregroundColor": hex_to_rgb("#0000ff")}}}}, "index": 0}})
        base_reqs.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": s_id, "startRowIndex": 2, "endRowIndex": l_row, "startColumnIndex": c_idx, "endColumnIndex": c_idx+1}],
            "booleanRule": {"condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": str(midpoint)}]}, "format": {"backgroundColor": hex_to_rgb("#f4cccc"), "textFormat": {"foregroundColor": hex_to_rgb("#ff0000")}}}}, "index": 0}})
    doc.batch_update({"requests": base_reqs})
    cross_ws.freeze(rows=2, cols=2)

    # グラフ描画（7日と15日を分離）
    try:
        min_v, max_v = (95, 105) if conf['mode'] == "機械割" else (-800, 800)
        
        def build_chart(title, start_col, anchor_row):
            series = []
            for c in range(start_col, start_col + 4):
                series.append({"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 1, "endRowIndex": l_row, "startColumnIndex": c, "endColumnIndex": c+1}]}}, "targetAxis": "LEFT_AXIS"})
            return {
                "addChart": {
                    "chart": {
                        "spec": {
                            "title": title,
                            "basicChart": {
                                "chartType": "LINE",
                                "legendPosition": "BOTTOM_LEGEND",
                                "axis": [
                                    {"position": "BOTTOM_AXIS", "title": "時系列"},
                                    {"position": "LEFT_AXIS", "title": conf['mode'], "viewWindowOptions": {"viewWindowMin": min_v, "viewWindowMax": max_v, "viewWindowMode": "EXPLICIT"}}
                                ],
                                "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 1, "endRowIndex": l_row, "startColumnIndex": 0, "endColumnIndex": 1}]}}}],
                                "series": series
                            }
                        },
                        "position": {"overlayPosition": {"anchorCell": {"sheetId": s_id, "rowIndex": anchor_row, "columnIndex": 14}}}
                    }
                }
            }

        graph_requests = [
            build_chart(f"【{conf['mode']}】7日移動平均トレンド", 6, 0),    # 7日MAは上段に
            build_chart(f"【{conf['mode']}】15日移動平均トレンド", 10, 15)  # 15日MAは15行目あたりから
        ]
        doc.batch_update({"requests": graph_requests})
        print("【成功】2枚のトレンドグラフを生成しました！")
    except Exception as ge:
        print(f"【エラー】グラフ描画に失敗しました: {ge}")

    print(f"\n--- 全分析工程が完了しました ---")

if __name__ == "__main__":
    asyncio.run(run_analysis())