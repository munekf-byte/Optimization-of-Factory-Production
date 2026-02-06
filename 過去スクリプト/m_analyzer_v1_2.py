# --- VERSION: m_analyzer_v1.2_Full_House_20260128 ---

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import asyncio
import sys

# ==========================================
# BLOCK: 1. 分析設定
# ==========================================
SPREADSHEET_KEY = "1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I"
RAW_DATA_SHEET  = "生データ"
OUTPUT_SHEET    = "Lパチスロ革命機ヴァルヴレイヴ2分析"
TARGET_STORE    = "学園の森" 
TARGET_MODEL    = "ヴァルヴレイヴ2" # キーワードで拾います
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
# BLOCK: 3. 分析エンジン（実況付き）
# ==========================================
async def run_analysis():
    print("\n--- [分析開始] ---")
    
    try:
        # 認証
        print("1. Google認証を開始します...")
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
        gc = gspread.authorize(creds)
        doc = gc.open_by_key(SPREADSHEET_KEY)
        print("   -> 認証成功。")

        # シート取得
        print(f"2. シート '{RAW_DATA_SHEET}' を読み込んでいます...")
        raw_ws = doc.worksheet(RAW_DATA_SHEET)
        
        # データ取得（ここが数分かかるはず）
        print(f"3. 生データをダウンロード中... (行数が多いと沈黙しますが待ってください)")
        all_data = raw_ws.get_all_values()
        print(f"   -> ダウンロード完了: {len(all_data)} 行取得しました。")

        if len(all_data) <= 1:
            print("【エラー】生データが空っぽです。")
            return

        print(f"4. '{TARGET_MODEL}' のデータを抽出中...")
        model_data_by_date = {}
        unique_units = set()
        
        # 抽出ロジック
        for i, row in enumerate(all_data[1:]):
            if i % 10000 == 0 and i > 0: print(f"   ... {i}行をスキャン済み")
            try:
                d_date, d_store, d_model, d_unit, d_diff, d_games = [col.strip() for col in row]
                if TARGET_STORE in d_store and TARGET_MODEL in d_model:
                    diff, games = int(d_diff), int(d_games)
                    unique_units.add(d_unit)
                    if d_date not in model_data_by_date:
                        model_data_by_date[d_date] = {}
                    model_data_by_date[d_date][d_unit] = {'diff': diff, 'games': games}
            except: continue

        if not unique_units:
            print(f"【警告】条件に合うデータが0件です。店名 '{TARGET_STORE}' または機種名 '{TARGET_MODEL}' を見直してください。")
            return

        print(f"5. 集計結果を計算中... (対象台数: {len(unique_units)}台)")
        # --- (計算・整形ロジックは以前の Ver.1.1 と同じ) ---
        sorted_units = sorted(list(unique_units), key=lambda x: int(x) if x.isdigit() else x)
        sorted_dates = sorted(model_data_by_date.keys(), reverse=False)

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

        print(f"6. 出力シート '{OUTPUT_SHEET}' へ書き込み中...")
        try:
            output_ws = doc.worksheet(OUTPUT_SHEET)
        except:
            output_ws = doc.add_worksheet(title=OUTPUT_SHEET, rows="1000", cols="100")
        
        output_ws.clear()
        output_ws.update(values=final_rows, range_name='A1')
        print("\n【完了】すべての処理が終わりました！")

    except Exception as e:
        print(f"\n【重大エラー】プログラムが途中で止まりました:\n{e}")

# ==========================================
# BLOCK: 5. 実行スイッチ
# ==========================================
if __name__ == "__main__":
    import re # ツールボックスのインポート漏れ防止
    asyncio.run(run_analysis())