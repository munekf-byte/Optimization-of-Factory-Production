# --- VERSION: m_analyzer_v1.3_Visual_Impact_20260128 ---

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
# BLOCK: 2. 道具箱
# ==========================================
def calculate_machine_rate(total_diff, total_games):
    if total_games == 0: return 0
    in_tokens = total_games * 3
    return round((in_tokens + total_diff) / in_tokens * 100, 1)

def clean_number(text):
    if not text or text == "-" or text == " " or text == "±0": return 0
    normalized = text.replace('▲', '-').replace('－', '-').replace(',', '').strip()
    match = re.search(r'(-?\d+)', normalized)
    return int(match.group(1)) if match else 0

# ==========================================
# BLOCK: 3. 分析・視覚化エンジン
# ==========================================
async def run_analysis():
    print("\n--- [分析＆視覚化 開始] ---")
    
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
        gc = gspread.authorize(creds)
        doc = gc.open_by_key(SPREADSHEET_KEY)
        raw_ws = doc.worksheet(RAW_DATA_SHEET)
        
        print("1. 生データをダウンロード中...")
        all_data = raw_ws.get_all_values()
        
        model_data_by_date = {}
        unique_units = set()
        
        print(f"2. {len(all_data)}行から '{TARGET_MODEL}' を抽出中...")
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

        # 統計行
        stats_10k = ["万枚突破率", "", "", "", "", "", ""]
        stats_5k  = ["5000枚突破率", "", "", "", "", "", ""]
        stats_avg = ["台個別平均差枚", "", "", "", "", "", ""]

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
            unit_diff_row = [day_units[unit]['diff'] if unit in day_units else "-" for unit in sorted_units]
            final_rows.append([date_str, day_of_week, t_diff, avg_diff, avg_games, m_rate, sticky_win_rate] + unit_diff_row)

        print(f"3. シート '{OUTPUT_SHEET}' へ書き込み中...")
        try:
            output_ws = doc.worksheet(OUTPUT_SHEET)
        except:
            output_ws = doc.add_worksheet(title=OUTPUT_SHEET, rows="1000", cols="100")
        
        output_ws.clear()
        output_ws.update(values=final_rows, range_name='A1')

        # --- 視覚化：フォーマット設定 ---
        print("4. 視覚化エフェクト（条件付き書式）を適用中...")
        
        # 行列の固定
        output_ws.freeze(rows=4, cols=7)

        # 条件付き書式のバッチ処理（標準のgspreadで可能な範囲で）
        rules = [
            # 万枚・5000枚突破率(上部)を黄色に
            {"range": "H1:Z2", "format": {"backgroundColor": {"red": 1.0, "green": 1.0, "blue": 0.8}}},
            # ヘッダー行をグレーに
            {"range": "A4:Z4", "format": {"backgroundColor": {"red": 0.8, "green": 0.8, "blue": 0.8}, "textFormat": {"bold": True}}},
            # 土日を薄い赤に
            {"range": "B5:B500", "format": {"textFormat": {"bold": True}}}
        ]
        
        # 個別差枚エリアへのヒートマップ適用は数が多いので、本来はAPIを直接叩きますが
        # まずは基本書式のみ適用し、リーダーの反応を見ます
        for rule in rules:
            output_ws.format(rule["range"], rule["format"])

        print("\n【完了】視覚化されたマトリックスが完成しました！")

    except Exception as e:
        print(f"エラー: {e}")

if __name__ == "__main__":
    import re
    asyncio.run(run_analysis())