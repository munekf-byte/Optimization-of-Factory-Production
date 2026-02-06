# --- VERSION: m_analyzer_v1.8_Perfect_Visual_Edition_20260129 ---

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
# BLOCK: 2. 道具箱（色彩変換エンジン）
# ==========================================
def hex_to_rgb(hex_str):
    if not hex_str: return {"red": 1, "green": 1, "blue": 1}
    hex_str = hex_str.lstrip('#')
    return {"red": int(hex_str[0:2], 16)/255.0, "green": int(hex_str[2:4], 16)/255.0, "blue": int(hex_str[4:6], 16)/255.0}

def get_style(value):
    """リーダー指定の9段階スタイルを判定"""
    try:
        v = int(value)
    except: return {"bg": "#ffffff", "text": "#000000", "bold": False}

    if v <= -3001:           return {"bg": "#f4cccc", "text": "#ff0000", "bold": True}
    elif -3000 <= v <= -1500: return {"bg": "#fff2cc", "text": "#ff0000", "bold": True}
    elif -1499 <= v <= -1:    return {"bg": "#fff2cc", "text": "#ff0000", "bold": False}
    elif 0 <= v <= 2000:      return {"bg": "#fff2cc", "text": "#000000", "bold": False}
    elif 2001 <= v <= 3499:   return {"bg": "#cfe2f3", "text": "#000000", "bold": False}
    elif 3500 <= v <= 4999:   return {"bg": "#9fc5e8", "text": "#000000", "bold": False}
    elif 5000 <= v <= 7999:   return {"bg": "#6fa8dc", "text": "#ffffff", "bold": True}
    elif 8000 <= v <= 11999:  return {"bg": "#3d85c6", "text": "#ffffff", "bold": True}
    elif v >= 12000:          return {"bg": "#0b5394", "text": "#ffffff", "bold": True}
    return {"bg": "#ffffff", "text": "#000000", "bold": False}

# ==========================================
# BLOCK: 3. 分析・視覚化エンジン
# ==========================================
async def run_analysis():
    print(f"\n--- [ {TARGET_MODEL} 最終視覚化を開始 ] ---")
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    gc = gspread.authorize(creds)
    doc = gc.open_by_key(SPREADSHEET_KEY)
    raw_ws = doc.worksheet(RAW_DATA_SHEET)
    
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

    # 統計計算
    t_all_d = sum(u['diff'] for d in model_data.values() for u in d.values())
    t_all_g = sum(u['games'] for d in model_data.values() for u in d.values())
    d_cnt, u_cnt = len(model_data), sum(len(d) for d in model_data.values())
    
    avg_total_d = int(t_all_d / d_cnt) if d_cnt > 0 else 0
    avg_unit_d = int(t_all_d / u_cnt) if u_cnt > 0 else 0
    avg_all_g = int(t_all_g / u_cnt) if u_cnt > 0 else 0
    avg_all_rate = round(((t_all_g * 3) + t_all_d) / (t_all_g * 3) * 100, 2) if t_all_g > 0 else 0

    # リーダー指定の座標配置
    row1 = ["万枚突破率", ""] + [""] * 5
    row2 = ["5000枚突破率", "", f"全期間総差枚平均: {avg_total_d}枚", f"全期間台平均: {avg_unit_d}枚", f"平均G: {avg_all_g}G", f"機械割: {avg_all_rate}%", ""]
    row3 = ["台個別平均差枚", ""] + [""] * 5

    for unit in sorted_units:
        diffs = [model_data[d][unit]['diff'] for d in sorted_dates if unit in model_data[d]]
        c = len(diffs)
        row1.append(f"{round(len([d for d in diffs if d >= 10000])/c*100, 1)}%" if c > 0 else "0%")
        row2.append(f"{round(len([d for d in diffs if d >= 5000])/c*100, 1)}%" if c > 0 else "0%")
        row3.append(f"{int(sum(diffs)/c)}枚" if c > 0 else "-")

    header = ["日付", "曜日", "機種総差枚", "台平均差枚", "平均G", "機械割", "粘り勝率"] + [f"{u}番台" for u in sorted_units]
    final_rows = [row1, row2, row3, header]

    for date_str in sorted_dates:
        day = model_data[date_str]
        dt = datetime.strptime(date_str, "%Y/%m/%d")
        dow = ["月", "火", "水", "木", "金", "土", "日"][dt.weekday()]
        t_d = sum(u['diff'] for u in day.values())
        t_g = sum(u['games'] for u in day.values())
        avg_d, avg_g = int(t_d/len(day)), int(t_g/len(day))
        m_rate = round(((t_g * 3) + t_d) / (t_g * 3) * 100, 2) if t_g > 0 else 0
        sticky = f"{round(len([u for u in day.values() if u['games'] >= STICKY_G_THRESHOLD and u['diff'] > 0])/len(day)*100, 1)}%"
        diff_row = [day[u]['diff'] if u in day else "-" for u in sorted_units]
        final_rows.append([date_str, dow, t_d, avg_d, f"{avg_g}G", f"{m_rate}%", sticky] + diff_row)

    try: out_ws = doc.worksheet(OUTPUT_SHEET)
    except: out_ws = doc.add_worksheet(title=OUTPUT_SHEET, rows="1000", cols="100")
    
    out_ws.clear()
    out_ws.update(values=final_rows, range_name='A1')

    # --- 視覚化魔法：バッチ処理 ---
    print("4. 設計図（Image 1）に従って色彩を適用中...")
    requests = []
    l_row, l_col = len(final_rows), len(header)
    sheet_id = out_ws.id

    # 全体中央揃え
    requests.append({"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": l_row, "startColumnIndex": 0, "endColumnIndex": l_col},
        "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE", "textFormat": {"fontSize": 10}}}, "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat.fontSize)"}})

    # 1-3行目統計エリア（薄黄色背景）
    requests.append({"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 3, "startColumnIndex": 0, "endColumnIndex": l_col},
        "cell": {"userEnteredFormat": {"backgroundColor": hex_to_rgb("#fff2cc"), "textFormat": {"bold": True}}}, "fields": "userEnteredFormat(backgroundColor,textFormat.bold)"}})

    # 4行目ヘッダー（グレー）
    requests.append({"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 3, "endRowIndex": 4, "startColumnIndex": 0, "endColumnIndex": l_col},
        "cell": {"userEnteredFormat": {"backgroundColor": hex_to_rgb("#cccccc"), "textFormat": {"bold": True}}}, "fields": "userEnteredFormat(backgroundColor,textFormat.bold)"}})

    # 数値エリアの色彩適用 (C, D列 ＋ H列以降)
    for r_idx, row_data in enumerate(final_rows[4:], start=4):
        # 曜日カラー
        dow_color = hex_to_rgb("#0000ff") if row_data[1] == "土" else hex_to_rgb("#ff0000") if row_data[1] == "日" else hex_to_rgb("#000000")
        requests.append({"updateCells": {"range": {"sheetId": sheet_id, "startRowIndex": r_idx, "endRowIndex": r_idx+1, "startColumnIndex": 0, "endColumnIndex": 2},
            "rows": [{"values": [{"userEnteredFormat": {"textFormat": {"foregroundColor": dow_color, "bold": True}}}, {"userEnteredFormat": {"textFormat": {"foregroundColor": dow_color, "bold": True}}}]}], "fields": "userEnteredFormat.textFormat"}})

        # 数値カラー適用
        for c_idx in [2, 3] + list(range(7, l_col)):
            val = row_data[c_idx]
            if isinstance(val, int):
                style = get_style(val)
                requests.append({"updateCells": {"range": {"sheetId": sheet_id, "startRowIndex": r_idx, "endRowIndex": r_idx+1, "startColumnIndex": c_idx, "endColumnIndex": c_idx+1},
                    "rows": [{"values": [{"userEnteredFormat": {"backgroundColor": hex_to_rgb(style["bg"]), "textFormat": {"foregroundColor": hex_to_rgb(style["text"]), "bold": style["bold"]}}}]}], "fields": "userEnteredFormat(backgroundColor,textFormat)"}})

    doc.batch_update({"requests": requests})
    out_ws.freeze(rows=4, cols=7)
    print("\n【完遂】真実の戦況図（Perfect Edition）が完成しました。")

if __name__ == "__main__":
    asyncio.run(run_analysis())