# --- VERSION: m_analyzer_v1_VVV_Edition_20260128 ---

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

# ==========================================
# BLOCK: 1. 分析設定
# ==========================================
SPREADSHEET_KEY = "1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I"
RAW_DATA_SHEET  = "生データ"
OUTPUT_SHEET    = "Lパチスロ革命機ヴァルヴレイヴ2分析"

# 分析対象の定義
TARGET_STORE = "学園の森" 
TARGET_MODEL = "Lパチスロ革命機ヴァルヴレイヴ2" # 生データにある正確な名称

# 粘り勝率の定義（例：5000G以上回されてプラスなら「粘り勝ち」）
STICKY_G_THRESHOLD = 5000

# ==========================================
# BLOCK: 2. 分析エンジン
# ==========================================
def calculate_machine_rate(total_diff, total_games):
    """機械割を計算する"""
    if total_games == 0: return 0
    # (投入枚数 + 差枚) / 投入枚数
    # 投入枚数 = G数 * 3
    in_tokens = total_games * 3
    return round((in_tokens + total_diff) / in_tokens * 100, 1)

async def run_analysis():
    # Google Sheets 認証
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    gc = gspread.authorize(creds)
    
    doc = gc.open_by_key(SPREADSHEET_KEY)
    raw_ws = doc.worksheet(RAW_DATA_SHEET)
    output_ws = doc.worksheet(OUTPUT_SHEET)

    print(f"[{TARGET_STORE}] の '{TARGET_MODEL}' を分析中...")
    
    # 1. 全生データを一気に読み込む
    all_data = raw_ws.get_all_values()
    if not all_data: return
    
    # 2. フィルタリングと台番号の特定
    model_data_by_date = {} # {日付: {台番: {diff, games}}}
    unique_units = set()    # 全ての台番号
    
    for row in all_data[1:]: # ヘッダーを飛ばす
        # 列構成: 0:日付, 1:店名, 2:機種名, 3:台番号, 4:差枚, 5:G数
        try:
            d_date, d_store, d_model, d_unit, d_diff, d_games = row
            if TARGET_STORE in d_store and d_model == TARGET_MODEL:
                unit_num = d_unit
                diff = int(d_diff)
                games = int(d_games)
                
                unique_units.add(unit_num)
                
                if d_date not in model_data_by_date:
                    model_data_by_date[d_date] = {}
                model_data_by_date[d_date][unit_num] = {'diff': diff, 'games': games}
        except: continue

    # 台番号を昇順に並べ替え
    sorted_units = sorted(list(unique_units), key=lambda x: int(x) if x.isdigit() else x)
    
    # 3. マトリックス作成
    final_rows = []
    
    # ヘッダー作成
    header = ["日付", "曜日", "総差枚", "平均G", "機械割", "粘り勝率"] + sorted_units
    final_rows.append(header)
    
    # 日付を新しい順に処理
    sorted_dates = sorted(model_data_by_date.keys(), reverse=True)
    
    for date_str in sorted_dates:
        day_units = model_data_by_date[date_str]
        
        # 曜日計算
        dt = datetime.strptime(date_str, "%Y/%m/%d")
        weekdays = ["月", "火", "水", "木", "金", "土", "日"]
        day_of_week = weekdays[dt.weekday()]
        
        # 指標計算
        total_diff = sum(u['diff'] for u in day_units.values())
        total_games = sum(u['games'] for u in day_units.values())
        avg_games = int(total_games / len(day_units)) if day_units else 0
        m_rate = calculate_machine_rate(total_diff, total_games)
        
        # 粘り勝率の計算
        sticky_wins = [u for u in day_units.values() if u['games'] >= STICKY_G_THRESHOLD and u['diff'] > 0]
        sticky_win_rate = f"{round(len(sticky_wins) / len(day_units) * 100, 1)}%" if day_units else "0%"
        
        # 台番号ごとの差枚を埋める
        unit_diffs = []
        for unit in sorted_units:
            if unit in day_units:
                unit_diffs.append(day_units[unit]['diff'])
            else:
                unit_diffs.append("-") # その日データがなかった台
        
        row_data = [date_str, day_of_week, total_diff, avg_games, m_rate, sticky_win_rate] + unit_diffs
        final_rows.append(row_data)

    # 4. 書き込み
    print(f"スプレッドシート '{OUTPUT_SHEET}' に書き込み中...")
    output_ws.clear()
    output_ws.append_rows(final_rows)
    print("分析完了！")

if __name__ == "__main__":
    import asyncio
    asyncio.run(run_analysis())