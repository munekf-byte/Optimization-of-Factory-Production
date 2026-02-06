# --- VERSION: m_analyzer_v1.1_Matrix_Master_20260128 ---

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import time

# ==========================================
# BLOCK: 1. 分析設定 (Ver 1.2 Robust Edition)
# ==========================================
SPREADSHEET_KEY = "1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I"
RAW_DATA_SHEET  = "生データ"
OUTPUT_SHEET    = "Lパチスロ革命機ヴァルヴレイヴ2分析" # タブ名を正確に
TARGET_STORE    = "学園の森" 
TARGET_MODEL    = "ヴァルヴレイヴ2" # 部分一致でOKにします
STICKY_G_THRESHOLD = 5000

# ==========================================
# BLOCK: 2. 道具箱
# ==========================================
def calculate_machine_rate(total_diff, total_games):
    if total_games == 0: return 0
    in_tokens = total_games * 3
    return round((in_tokens + total_diff) / in_tokens * 100, 1)

def get_color(value):
    """数値に応じて色（RGB）を返す。赤：プラス、青：マイナス"""
    try:
        val = int(value)
        if val >= 3000: return {"red": 1.0, "green": 0.8, "blue": 0.8} # 濃い赤
        if val > 0:    return {"red": 1.0, "green": 0.9, "blue": 0.9} # 薄い赤
        if val <= -3000: return {"red": 0.8, "green": 0.8, "blue": 1.0} # 濃い青
        if val < 0:    return {"red": 0.9, "green": 0.9, "blue": 1.0} # 薄い青
    except: pass
    return {"red": 1.0, "green": 1.0, "blue": 1.0}

# ==========================================
# BLOCK: 3. 分析実行 (判定ロジック強化版)
# ==========================================
async def run_analysis():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    gc = gspread.authorize(creds)
    doc = gc.open_by_key(SPREADSHEET_KEY)
    raw_ws = doc.worksheet(RAW_DATA_SHEET)
    
    try:
        output_ws = doc.worksheet(OUTPUT_SHEET)
    except:
        print(f"タブ '{OUTPUT_SHEET}' が見つかりません。作成します。")
        output_ws = doc.add_worksheet(title=OUTPUT_SHEET, rows="1000", cols="100")

    print(f"[{TARGET_STORE}] 内のキーワード '{TARGET_MODEL}' を検索中...")
    all_data = raw_ws.get_all_values()
    if not all_data: return
    
    model_data_by_date = {}
    unique_units = set()
    
    for row in all_data[1:]:
        try:
            # データの前後にある空白を掃除しながら取得
            d_date, d_store, d_model, d_unit, d_diff, d_games = [col.strip() for col in row]
            
            # 【重要】店名も機種名も「含まれているか」で判定（柔軟モード）
            if TARGET_STORE in d_store and TARGET_MODEL in d_model:
                diff, games = int(d_diff), int(d_games)
                unique_units.add(d_unit)
                if d_date not in model_data_by_date:
                    model_data_by_date[d_date] = {}
                model_data_by_date[d_date][d_unit] = {'diff': diff, 'games': games}
        except: continue

    if not unique_units:
        print(f"【警告】条件に合うデータが0件でした。店名 '{TARGET_STORE}' か機種名 '{TARGET_MODEL}' が生データと一致しているか確認してください。")
        return

    sorted_units = sorted(list(unique_units), key=lambda x: int(x) if x.isdigit() else x)
    sorted_dates = sorted(model_data_by_date.keys(), reverse=False)

    stats_10k = ["万枚突破率", "", "", "", "", ""]
    stats_5k  = ["5000枚突破率", "", "", "", "", ""]
    stats_avg = ["台個別平均差枚", "", "", "", "", ""]

    for unit in sorted_units:
        unit_diffs = [model_data_by_date[d][unit]['diff'] for d in sorted_dates if unit in model_data_by_date[d]]
        count = len(unit_diffs)
        if count > 0:
            avg = int(sum(unit_diffs) / count)
            r5k = f"{round(len([d for d in unit_diffs if d >= 5000]) / count * 100, 1)}%"
            r10k = f"{round(len([d for d in unit_diffs if d >= 10000]) / count * 100, 1)}%"
            stats_avg.append(avg); stats_5k.append(r5k); stats_10k.append(r10k)
        else:
            stats_avg.append("-"); stats_5k.append("-"); stats_10k.append("-")

    final_rows = [stats_10k, stats_5k, stats_avg]
    header = ["日付", "曜日", "機種総差枚", "台平均差枚", "平均G", "機械割", "粘り勝率"] + sorted_units
    final_rows.append(header)

    for date_str in sorted_dates:
        day_units = model_data_by_date[date_str]
        dt = datetime.strptime(date_str, "%Y/%m/%d")
        day_of_week = ["月", "火", "水", "木", "金", "土", "日"][dt.weekday()]
        t_diff = sum(u['diff'] for u in day_units.values())
        t_games = sum(u['games'] for u in day_units.values())
        avg_diff = int(t_diff / len(day_units)) if day_units else 0
        avg_games = int(t_games / len(day_units)) if day_units else 0
        m_rate = calculate_machine_rate(t_diff, total_games=t_games)
        sticky_wins = [u for u in day_units.values() if u['games'] >= STICKY_G_THRESHOLD and u['diff'] > 0]
        sticky_win_rate = f"{round(len(sticky_wins) / len(day_units) * 100, 1)}%" if day_units else "0%"
        
        unit_diff_row = []
        for unit in sorted_units:
            unit_diff_row.append(day_units[unit]['diff'] if unit in day_units else "-")
        final_rows.append([date_str, day_of_week, t_diff, avg_diff, avg_games, m_rate, sticky_win_rate] + unit_diff_row)

    print(f"スプレッドシート '{OUTPUT_SHEET}' へ {len(final_rows)}行を書き込み中...")
    output_ws.clear()
    output_ws.update(values=final_rows, range_name='A1') # 警告対策済みの書き方

    output_ws.format("A1:G3", {"textFormat": {"bold": True}})
    output_ws.format("A4:Z4", {"textFormat": {"bold": True}, "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}})
    print("分析完了！")