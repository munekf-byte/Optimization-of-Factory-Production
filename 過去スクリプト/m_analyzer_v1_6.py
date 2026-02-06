# --- VERSION: m_analyzer_v1.6_Color_Fix_20260129 ---

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
INDEX_SHEET     = "機種目録"
OUTPUT_SHEET    = "機種別分析" # 汎用的な分析用シート名にします

TARGET_STORE    = "学園の森" 
TARGET_MODEL    = "L東京喰種" # ここに機種名を入れる

STICKY_G_THRESHOLD = 5000

# ==========================================
# BLOCK: 2. 道具箱
# ==========================================
def calculate_machine_rate(total_diff, total_games):
    if total_games == 0: return 0
    return round(((total_games * 3) + total_diff) / (total_games * 3) * 100, 2)

def clean_number(text):
    if not text or text in ["-", " ", "±0"]: return 0
    text = str(text).replace('▲', '-').replace('－', '-').replace(',', '').strip()
    match = re.search(r'(-?\d+)', text)
    return int(match.group(1)) if match else 0

def num_to_col(n):
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string

# ==========================================
# BLOCK: 3. 機種目録更新
# ==========================================
def update_model_index(doc, all_data):
    print("--- 1. 機種目録を最新に更新中... ---")
    try:
        index_ws = doc.worksheet(INDEX_SHEET)
    except:
        index_ws = doc.add_worksheet(title=INDEX_SHEET, rows="1000", cols="5")
    
    model_list = {}
    for row in all_data[1:]:
        try:
            store, model, unit = row[1], row[2], row[3]
            if store not in model_list: model_list[store] = {}
            if model not in model_list[store]: model_list[store][model] = set()
            model_list[store][model].add(unit)
        except: continue
    
    index_rows = [["店舗名", "機種名", "累計設置台数"]]
    for store, models in model_list.items():
        for model, units in models.items():
            index_rows.append([store, model, len(units)])
    
    index_ws.clear()
    index_ws.update(values=index_rows, range_name='A1')
    print("   -> 完了。")

# ==========================================
# BLOCK: 4. 分析エンジン
# ==========================================
async def run_analysis():
    print(f"\n--- [ {TARGET_MODEL} 分析開始 ] ---")
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    gc = gspread.authorize(creds)
    doc = gc.open_by_key(SPREADSHEET_KEY)
    raw_ws = doc.worksheet(RAW_DATA_SHEET)
    
    all_data = raw_ws.get_all_values()
    update_model_index(doc, all_data)
    
    print(f"--- 2. マトリックス構築中... ---")
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

    if not unique_units:
        print("【エラー】データが見つかりませんでした。")
        return

    sorted_units = sorted(list(unique_units), key=lambda x: int(x) if x.isdigit() else x)
    sorted_dates = sorted(model_data.keys(), reverse=False)

    # 統計行
    stats_10k, stats_5k, stats_avg = ["万枚突破率", "", "", "", "", "", ""], ["5000枚突破率", "", "", "", "", "", ""], ["台個別平均差枚", "", "", "", "", "", ""]
    for unit in sorted_units:
        diffs = [model_data[d][unit]['diff'] for d in sorted_dates if unit in model_data[d]]
        cnt = len(diffs)
        stats_10k.append(f"{round(len([d for d in diffs if d >= 10000])/cnt*100, 1)}%" if cnt > 0 else "0%")
        stats_5k.append(f"{round(len([d for d in diffs if d >= 5000])/cnt*100, 1)}%" if cnt > 0 else "0%")
        stats_avg.append(f"{int(sum(diffs)/cnt)}枚" if cnt > 0 else "-")

    header = ["日付", "曜日", "機種総差枚", "台平均差枚", "平均G", "機械割", "粘り勝率"] + [f"{u}番台" for u in sorted_units]
    final_rows = [stats_10k, stats_5k, stats_avg, header]

    for date_str in sorted_dates:
        day = model_data[date_str]
        dt = datetime.strptime(date_str, "%Y/%m/%d")
        dow = ["月", "火", "水", "木", "金", "土", "日"][dt.weekday()]
        t_diff = sum(u['diff'] for u in day.values())
        t_games = sum(u['games'] for u in day.values())
        cnt = len(day)
        avg_d, avg_g = int(t_diff/cnt), int(t_games/cnt)
        m_rate = calculate_machine_rate(t_diff, t_games)
        sticky = f"{round(len([u for u in day.values() if u['games'] >= STICKY_G_THRESHOLD and u['diff'] > 0])/cnt*100, 1)}%"
        
        diff_row = [day[u]['diff'] if u in day else "-" for u in sorted_units]
        final_rows.append([date_str, dow, f"{t_diff}枚", f"{avg_d}枚", f"{avg_g}G", f"{m_rate}%", sticky] + diff_row)

    print(f"--- 3. シート '{OUTPUT_SHEET}' へ反映中... ---")
    try:
        out_ws = doc.worksheet(OUTPUT_SHEET)
    except:
        out_ws = doc.add_worksheet(title=OUTPUT_SHEET, rows="1000", cols="100")
    
    # 以前の書式とデータを完全にクリア
    out_ws.clear()
    out_ws.update(values=final_rows, range_name='A1')

    # 書式設定のバッチ処理
    last_col_idx = len(header)
    last_col_name = num_to_col(last_col_idx)
    last_row = len(final_rows)

    # 1. 全体中央揃え
    out_ws.format(f"A1:{last_col_name}{last_row}", {"horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"})
    
    # 2. 固定とヘッダー強調
    out_ws.freeze(rows=4, cols=7)
    out_ws.format("A1:G3", {"backgroundColor": {"red": 1.0, "green": 1.0, "blue": 0.8}, "textFormat": {"bold": True}})
    out_ws.format(f"A4:{last_col_name}4", {"backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}, "textFormat": {"bold": True}})

    # 3. 曜日カラー
    for i, row in enumerate(final_rows[4:], start=5):
        if row[1] == "土": out_ws.format(f"A{i}:B{i}", {"textFormat": {"foregroundColor": {"blue": 1.0}, "bold": True}})
        if row[1] == "日": out_ws.format(f"A{i}:B{i}", {"textFormat": {"foregroundColor": {"red": 1.0}, "bold": True}})

    # 4. 【修正完了】グラデーション設定 (H5から右下)
    # マイナス=赤(1, 0.7, 0.7), 0=白(1,1,1), プラス=青(0.7, 0.7, 1)
    body = {
        "requests": [
            {
                # 既存の条件付き書式を一旦全削除して、重複を防ぐ
                "clearBasicFilter": {"sheetId": out_ws.id}
            },
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{"sheetId": out_ws.id, "startRowIndex": 4, "endRowIndex": last_row, "startColumnIndex": 7, "endColumnIndex": last_col_idx}],
                        "gradientRule": {
                            "minpoint": {"color": {"red": 1.0, "green": 0.4, "blue": 0.4}, "type": "NUMBER", "value": "-5000"},
                            "midpoint": {"color": {"red": 1.0, "green": 1.0, "blue": 1.0}, "type": "NUMBER", "value": "0"},
                            "maxpoint": {"color": {"red": 0.4, "green": 0.4, "blue": 1.0}, "type": "NUMBER", "value": "5000"}
                        }
                    },
                    "index": 0
                }
            }
        ]
    }
    doc.batch_update(body)

    print(f"--- 分析完了！すべての工程が終了しました ---")

if __name__ == "__main__":
    asyncio.run(run_analysis())