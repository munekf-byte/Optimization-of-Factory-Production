# --- VERSION: m_analyzer_v1.4_Full_Visual_Commander_20260128 ---

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import asyncio

# ==========================================
# BLOCK: 1. 分析設定
# ==========================================
SPREADSHEET_KEY = "1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I"
RAW_DATA_SHEET  = "生データ"
OUTPUT_SHEET    = "Lパチスロ革命機ヴァルヴレイヴ2分析"
TARGET_STORE    = "学園の森" 
TARGET_MODEL    = "ヴァルヴレイヴ2"
STICKY_G_THRESHOLD = 5000

# ==========================================
# BLOCK: 2. 道具箱（単位付与と色彩設定）
# ==========================================
def calculate_machine_rate(total_diff, total_games):
    if total_games == 0: return 0
    in_tokens = total_games * 3
    return round((in_tokens + total_diff) / in_tokens * 100, 1)

def get_rgb(color_name):
    """色の定義"""
    colors = {
        "deep_red":   {"red": 1.0, "green": 0.6, "blue": 0.6}, # 大マイナス
        "light_red":  {"red": 1.0, "green": 0.9, "blue": 0.9}, # マイナス
        "deep_blue":  {"red": 0.6, "green": 0.6, "blue": 1.0}, # 大プラス
        "light_blue": {"red": 0.9, "green": 0.9, "blue": 1.0}, # プラス
        "sat_blue":   {"red": 0.0, "green": 0.0, "blue": 1.0}, # 土曜文字色
        "sun_red":    {"red": 1.0, "green": 0.0, "blue": 0.0}, # 日曜文字色
        "stats_yellow": {"red": 1.0, "green": 1.0, "blue": 0.8}, # 統計背景
        "gray":       {"red": 0.9, "green": 0.9, "blue": 0.9}  # ヘッダー
    }
    return colors.get(color_name)

# ==========================================
# BLOCK: 3. 分析・視覚化エンジン
# ==========================================
async def run_analysis():
    print("\n--- [分析＆フル視覚化 開始] ---")
    
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    gc = gspread.authorize(creds)
    doc = gc.open_by_key(SPREADSHEET_KEY)
    raw_ws = doc.worksheet(RAW_DATA_SHEET)
    
    print("1. 生データをスキャン中...")
    all_data = raw_ws.get_all_values()
    
    model_data_by_date = {}
    unique_units = set()
    
    for row in all_data[1:]:
        try:
            d_date, d_store, d_model, d_unit, d_diff, d_games = [col.strip() for col in row]
            if TARGET_STORE in d_store and TARGET_MODEL in d_model:
                diff, games = int(d_diff), int(d_games)
                unique_units.add(d_unit)
                if d_date not in model_data_by_date:
                    model_data_by_date[d_date] = {}
                model_data_by_date[d_date][d_unit] = {'diff': diff, 'games': games}
        except: continue

    sorted_units = sorted(list(unique_units), key=lambda x: int(x) if x.isdigit() else x)
    sorted_dates = sorted(model_data_by_date.keys(), reverse=False)

    # --- 統計行（単位付き） ---
    stats_10k = ["万枚突破率", "", "", "", "", "", ""]
    stats_5k  = ["5000枚突破率", "", "", "", "", "", ""]
    stats_avg = ["台個別平均差枚", "", "", "", "", "", ""]

    for unit in sorted_units:
        unit_diffs = [model_data_by_date[d][unit]['diff'] for d in sorted_dates if unit in model_data_by_date[d]]
        count = len(unit_diffs)
        if count > 0:
            avg = f"{int(sum(unit_diffs) / count)}枚"
            r5k = f"{round(len([d for d in unit_diffs if d >= 5000]) / count * 100, 1)}%"
            r10k = f"{round(len([d for d in unit_diffs if d >= 10000]) / count * 100, 1)}%"
            stats_avg.append(avg); stats_5k.append(r5k); stats_10k.append(r10k)
        else:
            stats_avg.append("-"); stats_5k.append("-"); stats_10k.append("-")

    # --- メインデータ構築 ---
    final_rows = [stats_10k, stats_5k, stats_avg]
    header = ["日付", "曜日", "機種総差枚", "台平均差枚", "平均G", "機械割", "粘り勝率"] + [f"{u}番台" for u in sorted_units]
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
        sticky_win_rate = f"{round(len(sticky_wins) / len(day_units) * 100, 1)}%"
        
        unit_diff_row = [day_units[unit]['diff'] if unit in day_units else "-" for unit in sorted_units]
        
        # 指標行に単位を付与
        final_rows.append([
            date_str, day_of_week, f"{t_diff}枚", f"{avg_diff}枚", f"{avg_games}G", f"{m_rate}%", sticky_win_rate
        ] + unit_diff_row)

    print(f"2. シート '{OUTPUT_SHEET}' へ書き込み中...")
    try:
        output_ws = doc.worksheet(OUTPUT_SHEET)
    except:
        output_ws = doc.add_worksheet(title=OUTPUT_SHEET, rows="1000", cols="100")
    
    output_ws.clear()
    output_ws.update(values=final_rows, range_name='A1')

    # --- 3. フォーマット一括適用 ---
    print("3. デザイン・色彩エフェクトを適用中...")
    
    # 基本設定：全セル中央揃え
    full_range = f"A1:Z{len(final_rows)}"
    output_ws.format(full_range, {
        "horizontalAlignment": "CENTER",
        "verticalAlignment": "MIDDLE",
        "textFormat": {"fontSize": 10}
    })

    # 固定設定
    output_ws.freeze(rows=4, cols=7)

    # ヘッダーと統計エリア
    output_ws.format("A1:G3", {"textFormat": {"bold": True}, "backgroundColor": get_rgb("stats_yellow")})
    output_ws.format("A4:Z4", {"textFormat": {"bold": True}, "backgroundColor": get_rgb("gray")})

    # 色彩ロジックの適用（差枚エリア H5以降）
    # 大量のセルがあるため、まずは主要なルールを適用します
    for i, row in enumerate(final_rows[4:], start=5):
        # 曜日の色
        dow = row[1]
        if dow == "土":
            output_ws.format(f"A{i}:B{i}", {"textFormat": {"foregroundColor": get_rgb("sat_blue"), "bold": True}})
        elif dow == "日":
            output_ws.format(f"A{i}:B{i}", {"textFormat": {"foregroundColor": get_rgb("sun_red"), "bold": True}})

    print("\n【完了】真実の戦況図が完成しました。")

if __name__ == "__main__":
    asyncio.run(run_analysis())