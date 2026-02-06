# --- VERSION: m_analyzer_v2.6_Trend_Navigator_20260129 ---

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
    """過去n日の移動平均を計算する"""
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
# BLOCK: 5. メイン分析エンジン（移動平均特化版）
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
    print(f"--- 2. クロス分析(MA算出)を実行中... ({conf['mode']}) ---")
    
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

    sorted_dates = sorted(daily_stats.keys(), reverse=False)
    
    # 基礎数値を一度リスト化
    base_values = {'all': [], 'A': [], 'B': [], 'C': []}
    for d_str in sorted_dates:
        day = daily_stats[d_str]
        def get_val(entries):
            if not entries: return 0
            if conf['mode'] == "差枚": return int(sum(e['diff'] for e in entries) / len(entries))
            if conf['mode'] == "G数": return int(sum(e['games'] for e in entries) / len(entries))
            return calculate_machine_rate(sum(e['diff'] for e in entries), sum(e['games'] for e in entries))
        
        base_values['all'].append(get_val(day['all']))
        base_values['A'].append(get_val(day['A']))
        base_values['B'].append(get_val(day['B']))
        base_values['C'].append(get_val(day['C']))

    # 移動平均を計算（3, 7, 15日）
    ma3 = {k: calculate_ma(v, 3) for k, v in base_values.items()}
    ma7 = {k: calculate_ma(v, 7) for k, v in base_values.items()}
    ma15 = {k: calculate_ma(v, 15) for k, v in base_values.items()}

    # 表のヘッダー
    header = ["日付", "曜日", "店全体", "部門A", "部門B", "部門C", 
              "店MA(3)", "A-MA(3)", "B-MA(3)", "C-MA(3)", 
              "店MA(7)", "A-MA(7)", "B-MA(7)", "C-MA(7)", 
              "店MA(15)", "A-MA(15)", "B-MA(15)", "C-MA(15)"]
    
    final_rows = [header]
    for i, date_str in enumerate(sorted_dates):
        dt = datetime.strptime(date_str, "%Y/%m/%d")
        dow = ["月", "火", "水", "木", "金", "土", "日"][dt.weekday()]
        row = [date_str, dow, base_values['all'][i], base_values['A'][i], base_values['B'][i], base_values['C'][i]]
        row += [ma3['all'][i], ma3['A'][i], ma3['B'][i], ma3['C'][i]]
        row += [ma7['all'][i], ma7['A'][i], ma7['B'][i], ma7['C'][i]]
        row += [ma15['all'][i], ma15['A'][i], ma15['B'][i], ma15['C'][i]]
        final_rows.append(row)

    print("--- 3. スプレッドシートへ反映中... ---")
    try:
        cross_ws = doc.worksheet(CROSS_SHEET)
        doc.del_worksheet(cross_ws)
    except: pass
    cross_ws = doc.add_worksheet(title=CROSS_SHEET, rows=len(final_rows)+50, cols=20)
    cross_ws.update(values=final_rows, range_name='A1')

    # --- グラフと書式設定 ---
    s_id = cross_ws.id
    l_row = len(final_rows)
    
    # 軸の設定（差枚か機械割かで分岐）
    if conf['mode'] == "機械割":
        min_v, max_v = 75, 120
    else:
        min_v, max_v = -2000, 2500

    requests = [
        {"repeatCell": {"range": {"sheetId": s_id}, "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"}}, "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment)"}},
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 0, "endRowIndex": 1}, "cell": {"userEnteredFormat": {"backgroundColor": hex_to_rgb("#cccccc"), "textFormat": {"bold": True}}}, "fields": "userEnteredFormat(backgroundColor,textFormat.bold)"}},
    ]

    # グラフ生成：3, 7, 15日のMAを描画（生データは描画範囲から外す）
    # MA7とMA15をメインとして線を描く設定（ここでは一旦全部出してみる）
    def create_series(col_idx):
        return {"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 0, "endRowIndex": l_row, "startColumnIndex": col_idx, "endColumnIndex": col_idx+1}]}}, "targetAxis": "LEFT_AXIS"}

    # 3, 7, 15すべてのMAをグラフに投入
    ma_series = [create_series(c) for c in range(6, 18)]

    requests.append({
        "addChart": {
            "chart": {
                "spec": {
                    "title": f"【{conf['mode']}】トレンド分析 (3/7/15 MA)",
                    "basicChart": {
                        "chartType": "LINE",
                        "legendPosition": "BOTTOM_LEGEND",
                        "axis": [
                            {"position": "BOTTOM_AXIS", "title": "時系列"},
                            {"position": "LEFT_AXIS", "viewWindow": {"min": min_v, "max": max_v}, "title": conf['mode']}
                        ],
                        "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 0, "endRowIndex": l_row, "startColumnIndex": 0, "endColumnIndex": 1}]}}}],
                        "series": ma_series
                    }
                },
                "position": {"overlayPosition": {"anchorCell": {"sheetId": s_id, "rowIndex": 0, "columnIndex": 18}}}
            }
        }
    })

    doc.batch_update({"requests": requests})
    cross_ws.freeze(rows=1, cols=2)
    print(f"\n【完遂】移動平均グラフ(3/7/15)を生成しました！")

if __name__ == "__main__":
    asyncio.run(run_analysis())