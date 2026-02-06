# --- VERSION: m_commander_v3.2_Final_Master_20260130 ---

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import asyncio
import csv
import time
import random
import re

# ==========================================
# BLOCK: 1. 固定設定
# ==========================================
SPREADSHEET_KEY = "1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I"
RAW_DATA_SHEET  = "生データ"
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
# BLOCK: 3. クロス分析（スマホ最適化・巨大グラフ）
# ==========================================
async def execute_cross_analysis(doc, conf, all_data):
    print("   [動作中] クロス分析を生成中...")
    daily_stats = {}
    for row in all_data[1:]:
        try:
            d_date, d_store, d_model, d_unit, d_diff, d_games = [c.strip() for c in row]
            if conf['store'] not in d_store: continue
            entry = {'diff': int(d_diff), 'games': int(d_games)}
            if d_date not in daily_stats: daily_stats[d_date] = {'all': [], 'A': [], 'B': [], 'C': []}
            daily_stats[d_date]['all'].append(entry)
            if d_model in conf['A_list']: daily_stats[d_date]['A'].append(entry)
            if d_model in conf['B_list']: daily_stats[d_date]['B'].append(entry)
            if d_model in conf['C_list']: daily_stats[d_date]['C'].append(entry)
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

    ma_results = {w: {k: calculate_ma(v, w) for k, v in base_vals.items()} for w in [3, 7, 15]}
    unit = "枚" if conf['mode'] == "差枚" else "G" if conf['mode'] == "G数" else "%"
    header = ["日付", "曜日", "店全体", conf['A_name'], conf['B_name'], conf['C_name']]
    for w in [3, 7, 15]: header += [f"店MA({w})", f"{conf['A_name']}({w})", f"{conf['B_name']}({w})", f"{conf['C_name']}({w})"]
    
    data_rows = []
    for i, d_str in enumerate(sorted_dates):
        dow = ["月", "火", "水", "木", "金", "土", "日"][datetime.strptime(d_str, "%Y/%m/%d").weekday()]
        row = [d_str, dow] + [f"{base_vals[k][i]}{unit}" for k in ['all', 'A', 'B', 'C']]
        for w in [3, 7, 15]: row += [ma_results[w][k][i] for k in ['all', 'A', 'B', 'C']]
        data_rows.append(row)

    try:
        ws = doc.worksheet(CROSS_SHEET)
        doc.del_worksheet(ws)
    except: pass
    ws = doc.add_worksheet(title=CROSS_SHEET, rows=len(data_rows)+100, cols=25)
    ws.update(values=[header] + data_rows, range_name='A50')

    s_id = ws.id
    l_row = len(data_rows) + 50
    min_v, max_v = (95, 105) if conf['mode'] == "機械割" else (1500, 8000) if conf['mode'] == "G数" else (-900, 900)
    
    def build_chart(title, start_col, anchor_row):
        colors, widths = ["#cccccc", "#ff0000", "#0000ff", "#00ff00"], [6, 2, 2, 2]
        series = []
        for i, c in enumerate(range(start_col, start_col + 4)):
            series.append({"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 49, "endRowIndex": l_row, "startColumnIndex": c, "endColumnIndex": c+1}]}},
                           "targetAxis": "LEFT_AXIS", "color": hex_to_rgb(colors[i]), "lineStyle": {"width": widths[i]}})
        return { "addChart": { "chart": { "spec": { "title": title, "basicChart": { "chartType": "LINE", "legendPosition": "BOTTOM_LEGEND",
                "axis": [{"position": "BOTTOM_AXIS"}, {"position": "LEFT_AXIS", "title": conf['mode'], "viewWindowOptions": {"viewWindowMin": min_v, "viewWindowMax": max_v, "viewWindowMode": "EXPLICIT"}}],
                "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 49, "endRowIndex": l_row, "startColumnIndex": 0, "endColumnIndex": 1}]}}}], "series": series } },
                "position": { "overlayPosition": { "anchorCell": { "sheetId": s_id, "rowIndex": anchor_row, "columnIndex": 0 }, "widthPixels": 1000, "heightPixels": 380 } } } } }

    reqs = [
        {"repeatCell": {"range": {"sheetId": s_id}, "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE", "wrapStrategy": "WRAP"}}, "fields": "userEnteredFormat"}},
        {"updateDimensionProperties": {"range": {"sheetId": s_id, "dimension": "COLUMNS", "startIndex": 6, "endIndex": 20}, "properties": {"pixelSize": 5}, "fields": "pixelSize"}}, 
        build_chart(f"【{conf['A_name']} / {conf['B_name']} / {conf['C_name']}】 3日トレンド", 6, 0),
        build_chart(f"7日トレンド", 10, 16),
        build_chart(f"15日トレンド", 14, 32)
    ]
    
    # 指標グラデーション設定
    def add_color(col, mid_val, step, is_percentage=False):
        c_idx = col
        return [
            {"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": s_id, "startRowIndex": 49, "endRowIndex": l_row, "startColumnIndex": c_idx, "endColumnIndex": c_idx+1}],
                "booleanRule": {"condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": str(mid_val)}]},
                "format": {"backgroundColor": hex_to_rgb("#cfe2f3"), "textFormat": {"foregroundColor": hex_to_rgb("#0000ff")}}}}, "index": 0}},
            {"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": s_id, "startRowIndex": 49, "endRowIndex": l_row, "startColumnIndex": c_idx, "endColumnIndex": c_idx+1}],
                "booleanRule": {"condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": str(mid_val)}]},
                "format": {"backgroundColor": hex_to_rgb("#f4cccc"), "textFormat": {"foregroundColor": hex_to_rgb("#ff0000")}}}}, "index": 0}}
        ]

    for c in range(2, 6): reqs += add_color(c, midpoint, 300)
    
    doc.batch_update({"requests": reqs})
    ws.freeze(rows=50, cols=2)

# ==========================================
# BLOCK: 4. 単独分析（精密マトリックス・凝縮版）
# ==========================================
async def execute_single_analysis(doc, conf, all_data):
    if not conf['target_model']: return
    print(f"   [動作中] 単独マトリックスを生成中: {conf['target_model']}")
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
    sorted_dates = sorted(model_data.keys(), reverse=False)

    # 1-3行目の統計
    t_all_d = sum(u['diff'] for d in model_data.values() for u in d.values())
    t_all_g = sum(u['games'] for d in model_data.values() for u in d.values())
    d_cnt, u_cnt = len(model_data), sum(len(d) for d in model_data.values())
    avg_total_d, avg_unit_d = int(t_all_d/d_cnt), int(t_all_d/u_cnt)
    avg_all_g = int(t_all_g/u_cnt)
    avg_all_rate = round(((t_all_g*3+t_all_d)/(t_all_g*3))*100, 2) if t_all_g > 0 else 0

    row1 = ["万枚突破率", ""] + [""] * 5
    row2 = ["5000枚突破率", "", f"総差枚平均: {avg_total_d}枚", f"台平均: {avg_unit_d}枚", f"平均G: {avg_all_g}G", f"機械割: {avg_all_rate}%", ""]
    row3 = ["台個別平均差枚", ""] + [""] * 5

    for unit in sorted_units:
        diffs = [model_data[d][unit]['diff'] for d in sorted_dates if unit in model_data[d]]
        c = len(diffs)
        row1.append(f"{round(len([d for d in diffs if d >= 10000])/c*100, 1)}%")
        row2.append(f"{round(len([d for d in diffs if d >= 5000])/c*100, 1)}%")
        row3.append(f"{int(sum(diffs)/c)}枚")

    header = ["日付", "曜日", "機種総差枚", "台平均差枚", "平均G", "機械割", "粘り勝率"] + [f"{u}番台" for u in sorted_units]
    data_rows = []
    for date_str in sorted_dates:
        day = model_data[date_str]
        dow = ["月", "火", "水", "木", "金", "土", "日"][datetime.strptime(date_str, "%Y/%m/%d").weekday()]
        t_d, t_g = sum(u['diff'] for u in day.values()), sum(u['games'] for u in day.values())
        avg_d, avg_g = int(t_d/len(day)), int(t_g/len(day))
        m_rate = round(((t_g * 3 + t_d) / (t_g * 3)) * 100, 2) if t_g > 0 else 0
        sticky = f"{round(len([u for u in day.values() if u['games']>=5000 and u['diff']>0])/len(day)*100, 1)}%"
        data_rows.append([date_str, dow, f"{t_d}枚", f"{avg_d}枚", f"{avg_g}G", f"{m_rate}%", sticky] + [day[u]['diff'] if u in day else "-" for u in sorted_units])

    try:
        ws = doc.worksheet(SINGLE_SHEET)
        doc.del_worksheet(ws)
    except: pass
    ws = doc.add_worksheet(title=SINGLE_SHEET, rows=len(data_rows)+50, cols=len(header)+10)
    ws.update(values=[row1, row2, row3, header] + data_rows, range_name='A1')
    
    s_id = ws.id
    l_row, l_col = len(data_rows) + 4, len(header)
    
    reqs = [
        {"repeatCell": {"range": {"sheetId": s_id}, "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE", "wrapStrategy": "WRAP", "textFormat": {"fontSize": 9}}}, "fields": "userEnteredFormat"}},
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 0, "endRowIndex": 3}, "cell": {"userEnteredFormat": {"backgroundColor": hex_to_rgb("#fff2cc"), "textFormat": {"bold": True}}}, "fields": "userEnteredFormat(backgroundColor,textFormat.bold)"}},
        {"updateDimensionProperties": {"range": {"sheetId": s_id, "dimension": "COLUMNS", "startIndex": 1, "endIndex": 2}, "properties": {"pixelSize": 30}, "fields": "pixelSize"}}, # 曜日
        {"updateDimensionProperties": {"range": {"sheetId": s_id, "dimension": "COLUMNS", "startIndex": 7, "endIndex": l_col}, "properties": {"pixelSize": 55}, "fields": "pixelSize"}}, # 台番
    ]
    # 9段階数式ルール
    target_ranges = [{"sheetId": s_id, "startRowIndex": 4, "endRowIndex": l_row, "startColumnIndex": 7, "endColumnIndex": l_col}]
    def add_f(formula, bg, text, bold):
        return {"addConditionalFormatRule": {"rule": {"ranges": target_ranges, "booleanRule": {"condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": formula}]}, "format": {"backgroundColor": hex_to_rgb(bg), "textFormat": {"foregroundColor": hex_to_rgb(text), "bold": bold}}}}, "index": 0}}
    
    reqs += [add_f("=H5<=-3001","#f4cccc","#ff0000",True), add_f("=AND(H5>=-3000,H5<=-1500)","#fff2cc","#ff0000",True), add_f("=AND(H5>=-1499,H5<=-1)","#fff2cc","#ff0000",False),
             add_f("=AND(H5>=0,H5<=2000)","#fff2cc","#000000",False), add_f("=AND(H5>=2001,H5<=3499)","#cfe2f3","#000000",False), add_f("=AND(H5>=3500,H5<=4999)","#9fc5e8","#000000",False),
             add_f("=AND(H5>=5000,H5<=7999)","#6fa8dc","#ffffff",True), add_f("=AND(H5>=8000,H5<=11999)","#3d85c6","#ffffff",True), add_f("=H5>=12000","#0b5394","#ffffff",True)]
    
    doc.batch_update({"requests": reqs})
    ws.freeze(rows=4, cols=7)

# ==========================================
# BLOCK: 5. 司令部（3系統ポーリング・信号機実況版）
# ==========================================
async def main():
    print("\n--- Python 司令部 起動プロセス開始 ---")
    try:
        print("1. Google認証キーを確認中...")
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
        
        print("2. Googleサーバーへ接続中...")
        gc = gspread.authorize(creds)
        
        print(f"3. スプレッドシートを捕捉...")
        doc = gc.open_by_key(SPREADSHEET_KEY)
        
        print(f"4. 司令塔シート '{CONFIG_SHEET}' を監視開始...")
        conf_ws = doc.worksheet(CONFIG_SHEET)

        print("\n--- Ver.3.2 統合司令室 待機中 ---")
        print(">> スマホから『実行』と入力されるのを監視しています...")

    except Exception as e:
        print(f"\n【初期設定エラー】接続状況やシートIDを確認してください:\n{e}")
        return

    while True:
        try:
            # 3つの実行ボタンを点呼
            all_cmd = conf_ws.acell('B2').value
            single_cmd = conf_ws.acell('C8').value
            cross_cmd = conf_ws.acell('C10').value
            
            # いずれかに「実行」があれば始動
            current_cmds = [str(all_cmd), str(single_cmd), str(cross_cmd)]
            if any("実行" in c for c in current_cmds):
                print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 指令を検知！")
                
                # 信号機：赤に変更（スマホへの合図）
                btn_cell = 'B2' if "実行" in str(all_cmd) else ('C8' if "実行" in str(single_cmd) else 'C10')
                conf_ws.format(btn_cell, {"backgroundColor": hex_to_rgb("#ff0000"), "textFormat": {"foregroundColor": hex_to_rgb("#ffffff")}})
                conf_ws.update_acell(btn_cell, "● 実行中")
                conf_ws.update_acell('B3', "データをロードしています...")

                # ローカルCSVから一括読込
                with open(LOCAL_DATABASE, mode='r', encoding='utf-8-sig') as f:
                    all_data = list(csv.reader(f))

                # 新レイアウトに基づき設定を読み取り
                vals = conf_ws.get_all_values()
                conf = {
                    "store": vals[4][1],
                    "target_model": vals[7][1],
                    "mode":  vals[9][1],   # 10行目: 比較指標
                    "A_name": vals[10][1], # 11行目: 部門A名
                    "A_list": [v[1] for v in vals[11:23] if v[1]], # 12-23行
                    "B_name": vals[23][1],
                    "B_list": [v[1] for v in vals[24:36] if v[1]],
                    "C_name": vals[36][1],
                    "C_list": [v[1] for v in vals[37:49] if v[1]]
                }
                
                # 実行判断
                if "実行" in str(all_cmd) or "実行" in str(cross_cmd):
                    await execute_cross_analysis(doc, conf, all_data)
                if "実行" in str(all_cmd) or "実行" in str(single_cmd):
                    await execute_single_analysis(doc, conf, all_data)
                
                # 信号機：緑に変更（完了の合図）
                finish_time = datetime.now().strftime('%H:%M')
                conf_ws.format(btn_cell, {"backgroundColor": hex_to_rgb("#00ff00"), "textFormat": {"foregroundColor": hex_to_rgb("#000000")}})
                conf_ws.update_acell(btn_cell, f"◎ 完了({finish_time})")
                conf_ws.update_acell('B3', f"前回の処理が正常に終了しました。")
                print(f"   [成功] {finish_time} 全ての分析を更新しました。")
                
                # スマホで確認する時間（15秒）待ってから黄色に戻す
                await asyncio.sleep(15)
                conf_ws.format(btn_cell, {"backgroundColor": hex_to_rgb("#ffff00")})
                conf_ws.update_acell(btn_cell, "待機中")

        except Exception as e:
            print(f"待機中エラー（ネット回線等を確認してください）: {e}")
            await asyncio.sleep(10)

        await asyncio.sleep(15)

if __name__ == "__main__":
    asyncio.run(main())