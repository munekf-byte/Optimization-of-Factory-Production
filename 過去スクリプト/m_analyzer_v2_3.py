# --- VERSION: m_analyzer_v2.3_Ultimate_Cross_20260129 ---

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
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
# BLOCK: 3. 新台順・機種目録更新エンジン
# ==========================================
def update_model_index_by_recency(doc, all_data):
    print("--- 1. 機種目録を『新台順』に更新中... ---")
    model_info = {} # {機種名: {'store':店名, 'last_date':最新日, 'units':セット}}
    
    for row in all_data[1:]:
        try:
            d_date, d_store, d_model, d_unit = row[0], row[1], row[2], row[3]
            if d_model not in model_info:
                model_info[d_model] = {'store': d_store, 'last_date': d_date, 'units': set()}
            model_info[d_model]['units'].add(d_unit)
            # より新しい日付があれば更新
            if d_date > model_info[d_model]['last_date']:
                model_info[d_model]['last_date'] = d_date
        except: continue
    
    # 日付の降順（新しい順）でソート
    sorted_models = sorted(model_info.items(), key=lambda x: x[1]['last_date'], reverse=True)
    
    index_rows = [["店舗名", "機種名", "設置台数", "最終確認日"]]
    for model_name, info in sorted_models:
        index_rows.append([info['store'], model_name, len(info['units']), info['last_date']])
    
    try:
        index_ws = doc.worksheet(INDEX_SHEET)
    except:
        index_ws = doc.add_worksheet(title=INDEX_SHEET, rows="1000", cols="5")
    
    index_ws.clear()
    index_ws.update(values=index_rows, range_name='A1')

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
    
    # 目録を新台順に更新（ここでGASのプルダウンが使いやすくなる）
    update_model_index_by_recency(doc, all_data)
    
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
            if d_model in conf['A']: daily_stats[d_date]['A'].append(entry)
            if d_model in conf['B']: daily_stats[d_date]['B'].append(entry)
            if d_model in conf['C']: daily_stats[d_date]['C'].append(entry)
        except: continue

    # 表作成
    header = ["日付", "曜日", "店全体", "部門A", "部門B", "部門C"]
    sorted_dates = sorted(daily_stats.keys(), reverse=False)
    
    data_rows = []
    # 総合計用
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
            totals[key].append(v if isinstance(v, (int, float)) else 0)
            return f"{v}%" if conf['mode'] == "機械割" else v

        day = daily_stats[date_str]
        data_rows.append([date_str, dow, calc_v(day['all'], 'all'), calc_v(day['A'], 'A'), calc_v(day['B'], 'B'), calc_v(day['C'], 'C')])

    # 総合平均行の作成
    def get_final_avg(lst):
        if not lst: return "-"
        avg = round(sum(lst)/len(lst), 2)
        return f"{avg}%" if conf['mode'] == "機械割" else int(avg)

    avg_row = ["総合平均", "-", get_final_avg(totals['all']), get_final_avg(totals['A']), get_final_avg(totals['B']), get_final_avg(totals['C'])]
    
    final_rows = [avg_row, header] + data_rows

    # 書き込み
    try:
        cross_ws = doc.worksheet(CROSS_SHEET)
        doc.del_worksheet(cross_ws)
    except: pass
    cross_ws = doc.add_worksheet(title=CROSS_SHEET, rows=len(final_rows)+50, cols=10)
    cross_ws.update(values=final_rows, range_name='A1')

    # デザインとグラフ（Batch Update）
    s_id = cross_ws.id
    requests = [
        # 中央揃え
        {"repeatCell": {"range": {"sheetId": s_id}, "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"}}, "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment)"}},
        # 総合平均行（1行目）を目立たせる
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 0, "endRowIndex": 1}, "cell": {"userEnteredFormat": {"backgroundColor": hex_to_rgb("#fff2cc"), "textFormat": {"bold": True, "fontSize": 11}}}, "fields": "userEnteredFormat(backgroundColor,textFormat)"}},
        # ヘッダー行（2行目）
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 1, "endRowIndex": 2}, "cell": {"userEnteredFormat": {"backgroundColor": hex_to_rgb("#cccccc"), "textFormat": {"bold": True}}}, "fields": "userEnteredFormat(backgroundColor,textFormat)"}}
    ]
    
    # グラフの追加
    requests.append({
        "addChart": {
            "chart": {
                "spec": {
                    "title": f"【{conf['mode']}】トレンド比較",
                    "basicChart": {
                        "chartType": "LINE",
                        "legendPosition": "BOTTOM_LEGEND",
                        "axis": [{"position": "BOTTOM_AXIS", "title": "時系列"}],
                        "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 1, "endRowIndex": len(final_rows), "startColumnIndex": 0, "endColumnIndex": 1}]}}}],
                        "series": [
                            {"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 1, "endRowIndex": len(final_rows), "startColumnIndex": 2, "endColumnIndex": 3}]}}, "targetAxis": "LEFT_AXIS"},
                            {"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 1, "endRowIndex": len(final_rows), "startColumnIndex": 3, "endColumnIndex": 4}]}}, "targetAxis": "LEFT_AXIS"},
                            {"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 1, "endRowIndex": len(final_rows), "startColumnIndex": 4, "endColumnIndex": 5}]}}, "targetAxis": "LEFT_AXIS"},
                            {"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 1, "endRowIndex": len(final_rows), "startColumnIndex": 5, "endColumnIndex": 6}]}}, "targetAxis": "LEFT_AXIS"}
                        ]
                    }
                },
                "position": {"newSheet": False, "overlayPosition": {"anchorCell": {"sheetId": s_id, "rowIndex": 0, "columnIndex": 7}, "offsetXPixels": 0, "offsetYPixels": 0}}
            }
        }
    })

    doc.batch_update({"requests": requests})
    cross_ws.freeze(rows=2)
    print(f"\n【完遂】『{CROSS_SHEET}』に総合平均とグラフを反映しました！")

if __name__ == "__main__":
    asyncio.run(run_analysis())