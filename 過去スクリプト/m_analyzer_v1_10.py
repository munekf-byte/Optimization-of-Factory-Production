# --- VERSION: m_analyzer_v1.10_Final_Visual_Edition_20260129 ---

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import asyncio
import re

# ==========================================
# BLOCK: 1. 分析設定
# ==========================================
SPREADSHEET_KEY = "1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I"
RAW_DATA_SHEET  = "生データ"
OUTPUT_SHEET    = "Lパチスロ革命機ヴァルヴレイヴ2分析"
TARGET_STORE    = "学園の森" 
TARGET_MODEL    = "Lパチスロ革命機ヴァルヴレイヴ2" 
STICKY_G_THRESHOLD = 5000

# ==========================================
# BLOCK: 2. 道具箱
# ==========================================
def hex_to_rgb(hex_str):
    hex_str = hex_str.lstrip('#')
    return {"red": int(hex_str[0:2], 16)/255.0, "green": int(hex_str[2:4], 16)/255.0, "blue": int(hex_str[4:6], 16)/255.0}

def num_to_col(n):
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string

# ==========================================
# BLOCK: 3. 分析・書き込みエンジン
# ==========================================
async def run_analysis():
    print(f"\n--- [ {TARGET_MODEL} 最終視覚化版を開始 ] ---")
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    gc = gspread.authorize(creds)
    doc = gc.open_by_key(SPREADSHEET_KEY)
    raw_ws = doc.worksheet(RAW_DATA_SHEET)
    
    print("1. 14万行のデータをロード中...")
    all_data = raw_ws.get_all_values()
    
    model_data = {}
    unique_units = set()
    for row in all_data[1:]:
        try:
            d_date, d_store, d_model, d_unit, d_diff, d_games = [c.strip() for c in row]
            if TARGET_STORE in d_store and TARGET_MODEL in d_model:
                diff, games = int(d_diff), int(d_games)
                unique_units.add(d_unit)
                if d_date not in model_data: model_data[d_date] = {}
                model_data[d_date][d_unit] = {'diff': diff, 'games': games}
        except: continue

    sorted_units = sorted(list(unique_units), key=lambda x: int(x) if x.isdigit() else x)
    sorted_dates = sorted(model_data.keys(), reverse=False)

    # 司令部指標の計算
    t_all_d = sum(u['diff'] for d in model_data.values() for u in d.values())
    t_all_g = sum(u['games'] for d in model_data.values() for u in d.values())
    d_cnt, u_cnt = len(model_data), sum(len(d) for d in model_data.values())
    avg_total_d, avg_unit_d = int(t_all_d / d_cnt), int(t_all_d / u_cnt)
    avg_all_g = int(t_all_g / u_cnt)
    avg_all_rate = round(((t_all_g * 3 + t_all_d) / (t_all_g * 3)) * 100, 2) if t_all_g > 0 else 0

    # リーダー指定の配置 (1-3行目)
    row1 = ["万枚突破率", ""] + [""] * 5
    row2 = ["5000枚突破率", "", f"{avg_total_d}枚", f"{avg_unit_d}枚", f"{avg_all_g}G", f"{avg_all_rate}%", ""]
    row3 = ["台個別平均差枚", ""] + [""] * 5

    for unit in sorted_units:
        diffs = [model_data[d][unit]['diff'] for d in sorted_dates if unit in model_data[d]]
        c = len(diffs)
        row1.append(f"{round(len([d for d in diffs if d >= 10000])/c*100, 1)}%")
        row2.append(f"{round(len([d for d in diffs if d >= 5000])/c*100, 1)}%")
        row3.append(f"{int(sum(diffs)/c)}枚")

    header = ["日付", "曜日", "機種総差枚", "台平均差枚", "平均G", "機械割", "粘り勝率"] + [f"{u}番台" for u in sorted_units]
    final_rows = [row1, row2, row3, header]

    for date_str in sorted_dates:
        day = model_data[date_str]
        dt = datetime.strptime(date_str, "%Y/%m/%d")
        dow = ["月", "火", "水", "木", "金", "土", "日"][dt.weekday()]
        t_d, t_g = sum(u['diff'] for u in day.values()), sum(u['games'] for u in day.values())
        avg_d, avg_g = int(t_d/len(day)), int(t_g/len(day))
        m_rate = round(((t_g * 3 + t_d) / (t_g * 3)) * 100, 2) if t_g > 0 else 0
        sticky = f"{round(len([u for u in day.values() if u['games'] >= STICKY_G_THRESHOLD and u['diff'] > 0])/len(day)*100, 1)}%"
        diff_row = [day[u]['diff'] if u in day else "-" for u in sorted_units]
        final_rows.append([date_str, dow, t_d, avg_d, avg_g, m_rate, sticky] + diff_row)

    print("2. シートをクリアしてデータを書き込み中...")
    try:
        out_ws = doc.worksheet(OUTPUT_SHEET)
    except:
        out_ws = doc.add_worksheet(title=OUTPUT_SHEET, rows=len(final_rows)+100, cols=len(header)+10)
    
    out_ws.clear()
    out_ws.update(values=final_rows, range_name='A1')

    # ==========================================
    # BLOCK: 4. 条件付き書式（一括バッチ設定）
    # ==========================================
    print("3. 9段階色彩ルールを適用中...（ここが最大の山場です）")
    s_id = out_ws.id
    l_row, l_col = len(final_rows), len(header)
    
    def create_bool_rule(min_v, max_v, bg, text, bold):
        """Google APIが確実に受け付ける形式でルールを作成"""
        if min_v == -99999: # 最小値以下
            cond = {"type": "NUMBER_LESS_EQUAL", "values": [{"userEnteredValue": str(max_v)}]}
        elif max_v == 99999: # 最大値以上
            cond = {"type": "NUMBER_GREATER_EQUAL", "values": [{"userEnteredValue": str(min_v)}]}
        else: # 範囲指定
            cond = {"type": "NUMBER_BETWEEN", "values": [{"userEnteredValue": str(min_v)}, {"userEnteredValue": str(max_v)}]}
        
        return {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": s_id, "startRowIndex": 4, "endRowIndex": l_row, "startColumnIndex": 2, "endColumnIndex": 4},
                               {"sheetId": s_id, "startRowIndex": 4, "endRowIndex": l_row, "startColumnIndex": 7, "endColumnIndex": l_col}],
                    "booleanRule": {
                        "condition": cond,
                        "format": {"backgroundColor": hex_to_rgb(bg), "textFormat": {"foregroundColor": hex_to_rgb(text), "bold": bold}}
                    }
                }, "index": 0
            }
        }

    rules = [
        create_bool_rule(-99999, -3001, "#f4cccc", "#ff0000", True),
        create_bool_rule(-3000, -1500, "#fff2cc", "#ff0000", True),
        create_bool_rule(-1499, -1, "#fff2cc", "#ff0000", False),
        create_bool_rule(0, 2000, "#fff2cc", "#000000", False),
        create_bool_rule(2001, 3499, "#cfe2f3", "#000000", False),
        create_bool_rule(3500, 4999, "#9fc5e8", "#000000", False),
        create_bool_rule(5000, 7999, "#6fa8dc", "#ffffff", True),
        create_bool_rule(8000, 11999, "#3d85c6", "#ffffff", True),
        create_bool_rule(12000, 99999, "#0b5394", "#ffffff", True),
    ]

    # 書式の初期設定（中央揃え、統計エリア、ヘッダー、土日）
    reqs = [
        {"repeatCell": {"range": {"sheetId": s_id}, "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"}}, "fields": "userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment"}},
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 0, "endRowIndex": 3}, "cell": {"userEnteredFormat": {"backgroundColor": hex_to_rgb("#fff2cc"), "textFormat": {"bold": True}}}, "fields": "userEnteredFormat(backgroundColor,textFormat.bold)"}},
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 3, "endRowIndex": 4}, "cell": {"userEnteredFormat": {"backgroundColor": hex_to_rgb("#cccccc"), "textFormat": {"bold": True}}}, "fields": "userEnteredFormat(backgroundColor,textFormat.bold)"}}
    ]

    # 土日の色塗り (A列とB列)
    for i, row in enumerate(final_rows[4:], start=4):
        dow = row[1]
        color = "#0000ff" if dow == "土" else "#ff0000" if dow == "日" else None
        if color:
            reqs.append({"updateCells": {"range": {"sheetId": s_id, "startRowIndex": i, "endRowIndex": i+1, "startColumnIndex": 0, "endColumnIndex": 2},
                "rows": [{"values": [{"userEnteredFormat": {"textFormat": {"foregroundColor": hex_to_rgb(color), "bold": True}}}, {"userEnteredFormat": {"textFormat": {"foregroundColor": hex_to_rgb(color), "bold": True}}}]}], "fields": "userEnteredFormat.textFormat"}})

    doc.batch_update({"requests": reqs + rules})
    out_ws.freeze(rows=4, cols=7)
    print("\n【完遂】軍師専用ダッシュボード（Ver.1.10）が完成しました！")

if __name__ == "__main__":
    asyncio.run(run_analysis())