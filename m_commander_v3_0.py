# --- VERSION: m_commander_v3.0_Auto_Waiting_20260130 ---

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import asyncio
import csv
import time
import random

# ==========================================
# BLOCK: 1. 固定設定
# ==========================================
SPREADSHEET_KEY = "1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I"
CONFIG_SHEET    = "分析設定"
CROSS_SHEET     = "クロス分析"
LOCAL_DATABASE  = "minrepo_database.csv"

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
# BLOCK: 3. 分析メインロジック（CSV読込型）
# ==========================================
async def execute_analysis(doc, conf):
    print(f"--- 分析実行開始: {conf['store']} ---")
    
    # 1. ローカルCSVからデータを高速読込
    all_data = []
    with open(LOCAL_DATABASE, mode='r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        all_data = list(reader)

    daily_stats = {}
    for row in all_data[1:]:
        try:
            d_date, d_store, d_model, d_unit, d_diff, d_games = [c.strip() for c in row]
            if conf['store'] not in d_store: continue
            
            entry = {'diff': int(d_diff), 'games': int(d_games)}
            if d_date not in daily_stats:
                daily_stats[d_date] = {'all': [], 'A': [], 'B': [], 'C': []}
            
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
            active = [e for e in entries if e['games'] > 0]
            if not active: return 0
            if conf['mode'] == "差枚": return int(sum(e['diff'] for e in active) / len(active))
            if conf['mode'] == "G数": return int(sum(e['games'] for e in active) / len(active))
            return calculate_machine_rate(sum(e['diff'] for e in active), sum(e['games'] for e in active))
        for k in base_vals.keys(): base_vals[k].append(get_v(day[k]))

    # 移動平均算出
    ma_results = {w: {k: calculate_ma(v, w) for k, v in base_vals.items()} for w in [3, 7, 15]}

    # 表データ構築
    unit = "枚" if conf['mode'] == "差枚" else "G" if conf['mode'] == "G数" else "%"
    header = ["日付", "曜日", "店全体", conf['A_name'], conf['B_name'], conf['C_name']]
    for w in [3, 7, 15]: header += [f"店MA({w})", f"{conf['A_name']}({w})", f"{conf['B_name']}({w})", f"{conf['C_name']}({w})"]
    
    data_rows = []
    for i, d_str in enumerate(sorted_dates):
        dow = ["月", "火", "水", "木", "金", "土", "日"][datetime.strptime(d_str, "%Y/%m/%d").weekday()]
        row = [d_str, dow]
        row += [f"{base_vals[k][i]}{unit}" for k in ['all', 'A', 'B', 'C']]
        for w in [3, 7, 15]: row += [ma_results[w][k][i] for k in ['all', 'A', 'B', 'C']]
        data_rows.append(row)

    final_rows = [["総合平均", "-", "", "", "", ""] + [""] * 12, header] + data_rows

    # スプレッドシートへ書き込み（クロス分析）
    try:
        ws = doc.worksheet(CROSS_SHEET)
        doc.del_worksheet(ws)
    except: pass
    ws = doc.add_worksheet(title=CROSS_SHEET, rows=len(final_rows)+50, cols=25)
    ws.update(values=final_rows, range_name='A1')

    # グラフ描画（Ver.2.12のロジックを継承、G数スケール修正済）
    s_id = ws.id
    l_row = len(final_rows)
    midpoint = 100 if conf['mode'] == "機械割" else 0
    
    # 軸のスケール調整（リーダー指定の神設定）
    if conf['mode'] == "機械割": min_v, max_v = 95, 105
    elif conf['mode'] == "G数": min_v, max_v = 1500, 8000
    else: min_v, max_v = -800, 800

    def build_chart(title, start_col, anchor_row):
        colors, widths = ["#cccccc", "#ff0000", "#0000ff", "#00ff00"], [6, 2, 2, 2]
        series = []
        for i, c in enumerate(range(start_col, start_col + 4)):
            series.append({"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 1, "endRowIndex": l_row, "startColumnIndex": c, "endColumnIndex": c+1}]}},
                           "targetAxis": "LEFT_AXIS", "color": hex_to_rgb(colors[i]), "lineStyle": {"width": widths[i]}})
        return { "addChart": { "chart": { "spec": { "title": title, "basicChart": { "chartType": "LINE", "legendPosition": "BOTTOM_LEGEND",
                "axis": [{"position": "BOTTOM_AXIS", "title": "時系列"}, {"position": "LEFT_AXIS", "title": conf['mode'], "viewWindowOptions": {"viewWindowMin": min_v, "viewWindowMax": max_v, "viewWindowMode": "EXPLICIT"}}],
                "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 1, "endRowIndex": l_row, "startColumnIndex": 0, "endColumnIndex": 1}]}}}], "series": series } },
                "position": { "overlayPosition": { "anchorCell": { "sheetId": s_id, "rowIndex": anchor_row, "columnIndex": 6 }, "widthPixels": 900, "heightPixels": 400 } } } } }

    reqs = [
        {"repeatCell": {"range": {"sheetId": s_id}, "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE", "wrapStrategy": "WRAP"}}, "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,wrapStrategy)"}},
        {"updateDimensionProperties": {"range": {"sheetId": s_id, "dimension": "COLUMNS", "startIndex": 6, "endIndex": 18}, "properties": {"pixelSize": 20}, "fields": "pixelSize"}},
        build_chart(f"【{conf['mode']}】 3日移動平均", 6, 0),
        build_chart(f"【{conf['mode']}】 7日移動平均", 10, 22),
        build_chart(f"【{conf['mode']}】 15日移動平均", 14, 44)
    ]
    doc.batch_update({"requests": reqs})
    ws.freeze(rows=2, cols=2)
    print("   -> 分析・グラフ生成完了。")

# ==========================================
# BLOCK: 4. 司令塔監視エンジン（メイン）
# ==========================================
async def main():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    gc = gspread.authorize(creds)
    doc = gc.open_by_key(SPREADSHEET_KEY)
    conf_ws = doc.worksheet(CONFIG_SHEET)

    print("\n--- Ver.3.0 司令室 待機中... ---")
    print("スマホから B10セル に『実行』と入力してください。")

    while True:
        try:
            # 1. 指令セル(B10)を確認
            trigger = conf_ws.acell('B10').value
            
            if trigger == "実行":
                print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 指令を検知！分析を開始します...")
                
                # 指令内容の読み込み
                vals = conf_ws.get_all_values()
                conf = {
                    "store": vals[1][1], "mode": vals[2][1],
                    "A_name": vals[3][1], "A_list": [v[1] for v in vals[4:14] if v[1]],
                    "B_name": vals[14][1], "B_list": [v[1] for v in vals[15:25] if v[1]],
                    "C_name": vals[25][1], "C_list": [v[1] for v in vals[26:36] if v[1]]
                }
                
                # 分析実行
                await execute_analysis(doc, conf)
                
                # 完了報告をセルに書き込む
                finish_time = datetime.now().strftime('%H:%M')
                conf_ws.update_acell('B10', f"完了 ({finish_time})")
                print(f"[{finish_time}] 全ての作業が完了しました。待機に戻ります。")

        except Exception as e:
            print(f"エラー発生(待機継続中): {e}")
            await asyncio.sleep(10) # エラー時は少し長めに待つ

        await asyncio.sleep(15) # 15秒おきに監視

if __name__ == "__main__":
    asyncio.run(main())