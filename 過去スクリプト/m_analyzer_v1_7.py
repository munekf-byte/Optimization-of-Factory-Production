# --- VERSION: m_analyzer_v1.7_Precision_Visuals_20260129 ---

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
OUTPUT_SHEET    = "機種別分析"
TARGET_STORE    = "学園の森" 
TARGET_MODEL    = "Lパチスロ革命機ヴァルヴレイヴ2" 
STICKY_G_THRESHOLD = 5000

# ==========================================
# BLOCK: 2. 道具箱（色彩変換エンジン）
# ==========================================
def hex_to_rgb(hex_str):
    """HexカラーコードをGoogle API用の0.0-1.0形式に変換"""
    hex_str = hex_str.lstrip('#')
    return {
        "red": int(hex_str[0:2], 16) / 255.0,
        "green": int(hex_str[2:4], 16) / 255.0,
        "blue": int(hex_str[4:6], 16) / 255.0
    }

def get_format_rule(value):
    """リーダー指定の9段階ルールを判定"""
    v = int(value)
    if v <= -3001:
        return {"bg": "#f4cccc", "text": "#ff0000", "bold": True}
    elif -3000 <= v <= -1500:
        return {"bg": "#fff2cc", "text": "#ff0000", "bold": True}
    elif -1499 <= v <= -1:
        return {"bg": "#fff2cc", "text": "#ff0000", "bold": False}
    elif 0 <= v <= 2000:
        return {"bg": "#fff2cc", "text": "#000000", "bold": False}
    elif 2001 <= v <= 3499:
        return {"bg": "#cfe2f3", "text": "#000000", "bold": False}
    elif 3500 <= v <= 4999:
        return {"bg": "#9fc5e8", "text": "#000000", "bold": False}
    elif 5000 <= v <= 7999:
        return {"bg": "#6fa8dc", "text": "#ffffff", "bold": True}
    elif 8000 <= v <= 11999:
        return {"bg": "#3d85c6", "text": "#ffffff", "bold": True}
    elif v >= 12000:
        return {"bg": "#0b5394", "text": "#ffffff", "bold": True}
    return {"bg": "#ffffff", "text": "#000000", "bold": False}

def num_to_col(n):
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string

# ==========================================
# BLOCK: 3. 分析エンジン
# ==========================================
async def run_analysis():
    print(f"\n--- [ {TARGET_MODEL} 精密分析開始 ] ---")
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    gc = gspread.authorize(creds)
    doc = gc.open_by_key(SPREADSHEET_KEY)
    raw_ws = doc.worksheet(RAW_DATA_SHEET)
    
    all_data = raw_ws.get_all_values()
    
    # 抽出
    model_data = {} # {date: {unit: {diff, games}}}
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

    # 全期間平均の計算
    total_all_diff = sum(u['diff'] for d in model_data.values() for u in d.values())
    total_all_games = sum(u['games'] for d in model_data.values() for u in d.values())
    all_days_count = len(model_data)
    all_units_count = sum(len(d) for d in model_data.values())
    
    avg_total_diff = int(total_all_diff / all_days_count) if all_days_count > 0 else 0
    avg_unit_diff = int(total_all_diff / all_units_count) if all_units_count > 0 else 0
    avg_all_games = int(total_all_games / all_units_count) if all_units_count > 0 else 0
    avg_all_rate = round(((total_all_games * 3) + total_all_diff) / (total_all_games * 3) * 100, 2) if total_all_games > 0 else 0

    # 1-3行目の統計エリア
    row1 = ["万枚突破率", "", f"全期間総差枚平均: {avg_total_diff}枚", "", "", "", ""]
    row2 = ["5000枚突破率", "", f"全期間台平均: {avg_unit_diff}枚", "", "", "", ""]
    row3 = ["台個別平均差枚", "", "", "", f"平均G: {avg_all_games}G", f"機械割: {avg_all_rate}%", ""]

    for unit in sorted_units:
        diffs = [model_data[d][unit]['diff'] for d in sorted_dates if unit in model_data[d]]
        cnt = len(diffs)
        row1.append(f"{round(len([d for d in diffs if d >= 10000])/cnt*100, 1)}%" if cnt > 0 else "0%")
        row2.append(f"{round(len([d for d in diffs if d >= 5000])/cnt*100, 1)}%" if cnt > 0 else "0%")
        row3.append(f"{int(sum(diffs)/cnt)}枚" if cnt > 0 else "-")

    header = ["日付", "曜日", "機種総差枚", "台平均差枚", "平均G", "機械割", "粘り勝率"] + [f"{u}番台" for u in sorted_units]
    final_rows = [row1, row2, row3, header]

    for date_str in sorted_dates:
        day = model_data[date_str]
        dt = datetime.strptime(date_str, "%Y/%m/%d")
        dow = ["月", "火", "水", "木", "金", "土", "日"][dt.weekday()]
        t_diff = sum(u['diff'] for u in day.values())
        t_games = sum(u['games'] for u in day.values())
        avg_d = int(t_diff/len(day))
        avg_g = int(t_games/len(day))
        m_rate = round(((t_games * 3) + t_diff) / (t_games * 3) * 100, 2) if t_games > 0 else 0
        sticky = f"{round(len([u for u in day.values() if u['games'] >= STICKY_G_THRESHOLD and u['diff'] > 0])/len(day)*100, 1)}%"
        diff_row = [day[u]['diff'] if u in day else "-" for u in sorted_units]
        final_rows.append([date_str, dow, t_diff, avg_d, avg_g, m_rate, sticky] + diff_row)

    # 書き込み
    try: out_ws = doc.worksheet(OUTPUT_SHEET)
    except: out_ws = doc.add_worksheet(title=OUTPUT_SHEET, rows="1000", cols="100")
    out_ws.clear()
    out_ws.update(values=final_rows, range_name='A1')

    # ==========================================
    # BLOCK: 4. 最強視覚化エフェクト
    # ==========================================
    print("--- 4. 視覚化魔法（バッチ処理）を適用中... ---")
    last_col = len(header)
    last_row = len(final_rows)
    
    requests = []
    
    # 全体中央揃え
    requests.append({
        "repeatCell": {
            "range": {"sheetId": out_ws.id, "startRowIndex": 0, "endRowIndex": last_row, "startColumnIndex": 0, "endColumnIndex": last_col},
            "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE", "textFormat": {"fontSize": 10}}},
            "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat.fontSize)"
        }
    })

    # 個別台エリア（H5以降）に9段階ルールを適用
    for r_idx, row_data in enumerate(final_rows[4:], start=4):
        for c_idx, val in enumerate(row_data[7:], start=7):
            if isinstance(val, int):
                rule = get_format_rule(val)
                requests.append({
                    "updateCells": {
                        "range": {"sheetId": out_ws.id, "startRowIndex": r_idx, "endRowIndex": r_idx+1, "startColumnIndex": c_idx, "endColumnIndex": c_idx+1},
                        "rows": [{"values": [{"userEnteredFormat": {
                            "backgroundColor": hex_to_rgb(rule["bg"]),
                            "textFormat": {"foregroundColor": hex_to_rgb(rule["text"]), "bold": rule["bold"]}
                        }}]}],
                        "fields": "userEnteredFormat(backgroundColor,textFormat)"
                    }
                })

    # 日本暦カラー（土曜:青, 日曜:赤）
    for r_idx, row_data in enumerate(final_rows[4:], start=4):
        dow = row_data[1]
        color = hex_to_rgb("#0000ff") if dow == "土" else hex_to_rgb("#ff0000") if dow == "日" else None
        if color:
            requests.append({
                "updateCells": {
                    "range": {"sheetId": out_ws.id, "startRowIndex": r_idx, "endRowIndex": r_idx+1, "startColumnIndex": 0, "endColumnIndex": 2},
                    "rows": [{"values": [{"userEnteredFormat": {"textFormat": {"foregroundColor": color, "bold": True}}}, {"userEnteredFormat": {"textFormat": {"foregroundColor": color, "bold": True}}}]}],
                    "fields": "userEnteredFormat.textFormat"
                }
            })

    doc.batch_update({"requests": requests})
    out_ws.freeze(rows=4, cols=7)
    print("\n【完遂】軍師専用ダッシュボードが完成しました。")

if __name__ == "__main__":
    asyncio.run(run_analysis())