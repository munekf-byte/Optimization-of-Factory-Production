# --- VERSION: m_analyzer_v2.4_UI_and_Graph_Fix_20260129 ---

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
# BLOCK: 3. 新・機種目録エンジン（人気順＆撤去区分）
# ==========================================
def update_model_index_v2(doc, all_data):
    print("--- 1. 機種目録を『人気順＋撤去区分』で更新中... ---")
    
    # 全体での最新日を特定
    all_dates = []
    for r in all_data[1:]:
        try: all_dates.append(datetime.strptime(r[0], "%Y/%m/%d"))
        except: continue
    max_date = max(all_dates) if all_dates else datetime.now()
    active_threshold = max_date - timedelta(days=7)

    model_stats = {} # {機種名: {last_date, total_g, days, units}}
    for row in all_data[1:]:
        try:
            d_date_str, d_store, d_model, d_unit, d_diff, d_games = row[0], row[1], row[2], row[3], row[4], row[5]
            dt = datetime.strptime(d_date_str, "%Y/%m/%d")
            if d_model not in model_stats:
                model_stats[d_model] = {'store': d_store, 'last_dt': dt, 'sum_g': 0, 'days': set(), 'units': set()}
            
            model_stats[d_model]['sum_g'] += int(d_games)
            model_stats[d_model]['days'].add(d_date_str)
            model_stats[d_model]['units'].add(d_unit)
            if dt > model_stats[d_model]['last_dt']:
                model_stats[d_model]['last_dt'] = dt
        except: continue

    # 並び替えロジック
    active_list = []
    withdrawn_list = []

    for name, s in model_stats.items():
        avg_g = int(s['sum_g'] / len(s['days'])) if s['days'] else 0
        info = [s['store'], name, len(s['units']), s['last_dt'].strftime("%Y/%m/%d"), avg_g]
        
        if s['last_dt'] >= active_threshold:
            active_list.append(info)
        else:
            info[1] = f"[撤去] {info[1]}"
            withdrawn_list.append(info)

    # 現役は平均G数順、撤去は日付順にソート
    active_list.sort(key=lambda x: x[4], reverse=True)
    withdrawn_list.sort(key=lambda x: x[3], reverse=True)

    index_rows = [["店舗名", "機種名", "設置台数", "最終稼働日", "平均稼働G"]] + active_list + withdrawn_list
    
    try:
        index_ws = doc.worksheet(INDEX_SHEET)
    except:
        index_ws = doc.add_worksheet(title=INDEX_SHEET, rows="1000", cols="5")
    
    index_ws.clear()
    index_ws.update(values=index_rows, range_name='A1')
    print(f"   -> 完了。現役 {len(active_list)} 機種 / 撤去 {len(withdrawn_list)} 機種")

# ==========================================
# BLOCK: 4. 分析設定読み込み
# ==========================================
def get_config_v2(doc):
    conf_ws = doc.worksheet(CONFIG_SHEET)
    vals = conf_ws.get_all_values()
    return {
        "store": vals[1][1],
        "mode":  vals[2][1],
        "A": [v[1] for v in vals[4:14] if v[1]],
        "B": [v[1] for v in vals[15:25] if v[1]],
        "C": [v[1] for v in vals[26:36] if v[1]]
    }

# ==========================================
# BLOCK: 5. メイン分析・グラフ生成エンジン
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
            if d_date not in daily_stats:
                daily_stats[d_date] = {'all': [], 'A': [], 'B': [], 'C': []}
            daily_stats[d_date]['all'].append(entry)
            
            # [撤去] タグが付いている場合を考慮して部分一致で判定
            clean_model = d_model.replace("[撤去] ", "")
            if any(clean_model in m for m in conf['A']): daily_stats[d_date]['A'].append(entry)
            if any(clean_model in m for m in conf['B']): daily_stats[d_date]['B'].append(entry)
            if any(clean_model in m for m in conf['C']): daily_stats[d_date]['C'].append(entry)
        except: continue

    header = ["日付", "曜日", "店全体", "部門A", "部門B", "部門C"]
    sorted_dates = sorted(daily_stats.keys(), reverse=False)
    
    data_rows = []
    totals = {'all': [], 'A': [], 'B': [], 'C': []}

    for date_str in sorted_dates:
        dt = datetime.strptime(date_str, "%Y/%m/%d")
        dow = ["月", "火", "水", "木", "金", "土", "日"][dt.weekday()]
        
        def calc_v(entries, key):
            if not entries: return "-"
            if conf['mode'] == "差枚":
                v = int(sum(e['diff'] for e in entries) / len(entries))
            elif conf['mode'] == "G数":
                v = int(sum(e['games'] for e in entries) / len(entries))
            else: # 機械割
                v = calculate_machine_rate(sum(e['diff'] for e in entries), sum(e['games'] for e in entries))
            totals[key].append(v)
            return v

        day = daily_stats[date_str]
        data_rows.append([date_str, dow, calc_v(day['all'], 'all'), calc_v(day['A'], 'A'), calc_v(day['B'], 'B'), calc_v(day['C'], 'C')])

    # 総合平均
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

    # --- 書式とグラフ（一括） ---
    print("--- 3. グラフ生成と視覚化を適用中... ---")
    s_id = cross_ws.id
    l_row = len(final_rows)
    
    requests = [
        {"repeatCell": {"range": {"sheetId": s_id}, "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"}}, "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment)"}},
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 0, "endRowIndex": 1}, "cell": {"userEnteredFormat": {"backgroundColor": hex_to_rgb("#fff2cc"), "textFormat": {"bold": True}}}, "fields": "userEnteredFormat(backgroundColor,textFormat.bold)"}},
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 1, "endRowIndex": 2}, "cell": {"userEnteredFormat": {"backgroundColor": hex_to_rgb("#cccccc"), "textFormat": {"bold": True}}}, "fields": "userEnteredFormat(backgroundColor,textFormat.bold)"}},
        # グラフ作成（エラー修正版）
        {
            "addChart": {
                "chart": {
                    "spec": {
                        "title": f"【{conf['mode']}】トレンド比較",
                        "basicChart": {
                            "chartType": "LINE",
                            "legendPosition": "BOTTOM_LEGEND",
                            "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 1, "endRowIndex": l_row, "startColumnIndex": 0, "endColumnIndex": 1}]}}}],
                            "series": [
                                {"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 1, "endRowIndex": l_row, "startColumnIndex": 2, "endColumnIndex": 6}]}}, "targetAxis": "LEFT_AXIS"}
                            ]
                        }
                    },
                    "position": {
                        "overlayPosition": {
                            "anchorCell": {"sheetId": s_id, "rowIndex": 0, "columnIndex": 7},
                            "offsetXPixels": 0, "offsetYPixels": 0
                        }
                    }
                }
            }
        }
    ]

    # 土日色塗り
    for i, row in enumerate(data_rows, start=2):
        color = "#0000ff" if row[1] == "土" else "#ff0000" if row[1] == "日" else None
        if color:
            requests.append({"updateCells": {"range": {"sheetId": s_id, "startRowIndex": i, "endRowIndex": i+1, "startColumnIndex": 0, "endColumnIndex": 2},
                "rows": [{"values": [{"userEnteredFormat": {"textFormat": {"foregroundColor": hex_to_rgb(color), "bold": True}}}, {"userEnteredFormat": {"textFormat": {"foregroundColor": hex_to_rgb(color), "bold": True}}}]}], "fields": "userEnteredFormat.textFormat"}})

    doc.batch_update({"requests": requests})
    cross_ws.freeze(rows=2)
    print(f"\n【完遂】トレンドグラフの描画に成功しました！")

if __name__ == "__main__":
    asyncio.run(run_analysis())