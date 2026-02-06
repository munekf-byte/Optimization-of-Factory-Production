# --- VERSION: m_analyzer_v2.5_Graph_and_Color_Fixed_20260129 ---

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

# ==========================================
# BLOCK: 3. 機種目録エンジン
# ==========================================
def update_model_index_v2(doc, all_data):
    print("--- 1. 機種目録を更新中... ---")
    all_dates = []
    for r in all_data[1:]:
        try: all_dates.append(datetime.strptime(r[0], "%Y/%m/%d"))
        except: continue
    max_date = max(all_dates) if all_dates else datetime.now()
    active_threshold = max_date - timedelta(days=7)

    model_stats = {}
    for row in all_data[1:]:
        try:
            d_date_str, d_store, d_model, d_unit, d_diff, d_games = row[0], row[1], row[2], row[3], row[4], row[5]
            dt = datetime.strptime(d_date_str, "%Y/%m/%d")
            if d_model not in model_stats:
                model_stats[d_model] = {'store': d_store, 'last_dt': dt, 'sum_g': 0, 'days': set(), 'units': set()}
            model_stats[d_model]['sum_g'] += int(d_games)
            model_stats[d_model]['days'].add(d_date_str)
            model_stats[d_model]['units'].add(d_unit)
            if dt > model_stats[d_model]['last_dt']: model_stats[d_model]['last_dt'] = dt
        except: continue

    active_list, withdrawn_list = [], []
    for name, s in model_stats.items():
        avg_g = int(s['sum_g'] / len(s['days'])) if s['days'] else 0
        info = [s['store'], name, len(s['units']), s['last_dt'].strftime("%Y/%m/%d"), avg_g]
        if s['last_dt'] >= active_threshold: active_list.append(info)
        else: info[1] = f"[撤去] {info[1]}"; withdrawn_list.append(info)

    active_list.sort(key=lambda x: x[4], reverse=True)
    withdrawn_list.sort(key=lambda x: x[3], reverse=True)
    index_rows = [["店舗名", "機種名", "設置台数", "最終稼働日", "平均稼働G"]] + active_list + withdrawn_list
    
    try: index_ws = doc.worksheet(INDEX_SHEET)
    except: index_ws = doc.add_worksheet(title=INDEX_SHEET, rows="1000", cols="5")
    index_ws.clear()
    index_ws.update(values=index_rows, range_name='A1')

# ==========================================
# BLOCK: 4. 分析設定読み込み
# ==========================================
def get_config_v2(doc):
    conf_ws = doc.worksheet(CONFIG_SHEET)
    vals = conf_ws.get_all_values()
    return {
        "store": vals[1][1], "mode":  vals[2][1],
        "A": [v[1] for v in vals[4:14] if v[1]],
        "B": [v[1] for v in vals[15:25] if v[1]],
        "C": [v[1] for v in vals[26:36] if v[1]]
    }

# ==========================================
# BLOCK: 5. メインエンジン（グラフ・色彩修正版）
# ==========================================
async def run_analysis():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    gc = gspread.authorize(creds)
    doc = gc.open_by_key(SPREADSHEET_KEY)
    
    raw_ws = doc.worksheet(RAW_DATA_SHEET)
    all_data = raw_ws.get_all_values()
    
    update_model_index_v2(doc, all_data)
    conf = get_config_v2(doc)
    print(f"--- 2. クロス分析を実行中... ({conf['mode']}) ---")
    
    daily_stats = {}
    for row in all_data[1:]:
        try:
            d_date, d_store, d_model, d_unit, d_diff, d_games = [c.strip() for c in row]
            if conf['store'] not in d_store: continue
            entry = {'diff': int(d_diff), 'games': int(d_games)}
            if d_date not in daily_stats: daily_stats[d_date] = {'all': [], 'A': [], 'B': [], 'C': []}
            daily_stats[d_date]['all'].append(entry)
            
            clean_model = d_model.replace("[撤去] ", "")
            if any(clean_model == m for m in conf['A']): daily_stats[d_date]['A'].append(entry)
            if any(clean_model == m for m in conf['B']): daily_stats[d_date]['B'].append(entry)
            if any(clean_model == m for m in conf['C']): daily_stats[d_date]['C'].append(entry)
        except: continue

    header = ["日付", "曜日", "店全体", "部門A", "部門B", "部門C"]
    sorted_dates = sorted(daily_stats.keys(), reverse=False)
    
    data_rows, totals = [], {'all': [], 'A': [], 'B': [], 'C': []}

    for date_str in sorted_dates:
        dt = datetime.strptime(date_str, "%Y/%m/%d")
        dow = ["月", "火", "水", "木", "金", "土", "日"][dt.weekday()]
        
        def calc_v(entries, key):
            if not entries: return "-"
            if conf['mode'] == "差枚": v = int(sum(e['diff'] for e in entries) / len(entries))
            elif conf['mode'] == "G数": v = int(sum(e['games'] for e in entries) / len(entries))
            else: v = calculate_machine_rate(sum(e['diff'] for e in entries), sum(e['games'] for e in entries))
            totals[key].append(v); return v

        day = daily_stats[date_str]
        data_rows.append([date_str, dow, calc_v(day['all'], 'all'), calc_v(day['A'], 'A'), calc_v(day['B'], 'B'), calc_v(day['C'], 'C')])

    def get_final_avg(lst):
        if not lst: return "-"
        avg = round(sum(lst)/len(lst), 2)
        return f"{avg}%" if conf['mode'] == "機械割" else int(avg)

    avg_row = ["総合平均", "-", get_final_avg(totals['all']), get_final_avg(totals['A']), get_final_avg(totals['B']), get_final_avg(totals['C'])]
    final_rows = [avg_row, header] + data_rows

    try:
        cross_ws = doc.worksheet(CROSS_SHEET)
        doc.del_worksheet(cross_ws)
    except: pass
    cross_ws = doc.add_worksheet(title=CROSS_SHEET, rows=len(final_rows)+50, cols=20)
    cross_ws.update(values=final_rows, range_name='A1')

    # --- 書式とグラフ設定 ---
    print("--- 3. グラフ生成と色彩設定を適用中... ---")
    s_id = cross_ws.id
    l_row = len(final_rows)
    
    # 基本リクエスト（中央揃え、統計背景、ヘッダー背景）
    requests = [
        {"repeatCell": {"range": {"sheetId": s_id}, "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"}}, "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment)"}},
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 0, "endRowIndex": 1}, "cell": {"userEnteredFormat": {"backgroundColor": hex_to_rgb("#fff2cc"), "textFormat": {"bold": True}}}, "fields": "userEnteredFormat(backgroundColor,textFormat.bold)"}},
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 1, "endRowIndex": 2}, "cell": {"userEnteredFormat": {"backgroundColor": hex_to_rgb("#cccccc"), "textFormat": {"bold": True}}}, "fields": "userEnteredFormat(backgroundColor,textFormat.bold)"}},
    ]

    # 【重要】グラフの各線を個別に定義 (店全体:2, A:3, B:4, C:5)
    series_list = []
    for col_idx in range(2, 6):
        series_list.append({
            "series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 1, "endRowIndex": l_row, "startColumnIndex": col_idx, "endColumnIndex": col_idx+1}]}},
            "targetAxis": "LEFT_AXIS"
        })

    requests.append({
        "addChart": {
            "chart": {
                "spec": {
                    "title": f"【{conf['mode']}】トレンド比較",
                    "basicChart": {
                        "chartType": "LINE",
                        "legendPosition": "BOTTOM_LEGEND",
                        "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 1, "endRowIndex": l_row, "startColumnIndex": 0, "endColumnIndex": 1}]}}}],
                        "series": series_list
                    }
                },
                "position": {"overlayPosition": {"anchorCell": {"sheetId": s_id, "rowIndex": 0, "columnIndex": 7}}}
            }
        }
    })

    # 【重要】色彩設定（店平均より上なら青、下なら赤）を全列(C〜F)に適用
    for col_idx in range(2, 6):
        col_letter = chr(65 + col_idx)
        # プラス乖離：青系
        requests.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": s_id, "startRowIndex": 2, "endRowIndex": l_row, "startColumnIndex": col_idx, "endColumnIndex": col_idx+1}],
            "booleanRule": {"condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": "0"}]},
            "format": {"backgroundColor": hex_to_rgb("#cfe2f3"), "textFormat": {"foregroundColor": hex_to_rgb("#0000ff")}}}}, "index": 0}})
        # マイナス乖離：赤系
        requests.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": s_id, "startRowIndex": 2, "endRowIndex": l_row, "startColumnIndex": col_idx, "endColumnIndex": col_idx+1}],
            "booleanRule": {"condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": "0"}]},
            "format": {"backgroundColor": hex_to_rgb("#f4cccc"), "textFormat": {"foregroundColor": hex_to_rgb("#ff0000")}}}}, "index": 0}})

    doc.batch_update({"requests": requests})
    cross_ws.freeze(rows=2)
    print(f"\n【完遂】トレンドグラフと勝敗の色彩が反映されました！")

if __name__ == "__main__":
    import asyncio
    asyncio.run(run_analysis())