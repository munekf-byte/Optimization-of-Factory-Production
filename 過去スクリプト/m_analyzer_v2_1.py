# --- VERSION: m_analyzer_v2.1_Command_Center_Fixed_20260129 ---

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import asyncio
import re

# ==========================================
# BLOCK: 1. 固定設定
# ==========================================
SPREADSHEET_KEY = "1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I"
RAW_DATA_SHEET  = "生データ"
CONFIG_SHEET    = "分析設定"
CROSS_SHEET     = "クロス分析"

# ==========================================
# BLOCK: 2. 道具箱
# ==========================================
def hex_to_rgb(hex_str):
    hex_str = hex_str.lstrip('#')
    return {"red": int(hex_str[0:2], 16)/255.0, "green": int(hex_str[2:4], 16)/255.0, "blue": int(hex_str[4:6], 16)/255.0}

# ==========================================
# BLOCK: 3. 設定読み込みエンジン（ヘッダー考慮版）
# ==========================================
def get_config(doc):
    print("--- 1. 司令塔（分析設定）から指示を読み込み中... ---")
    conf_ws = doc.worksheet(CONFIG_SHEET)
    vals = conf_ws.get_all_values()
    
    # 1行目は見出しなので、vals[1]が「店舗名」、vals[8]が「比較指標」になる
    config = {
        "store":        vals[1][1], # 2行目・B列
        "group_a_name": vals[2][1], # 3行目・B列
        "group_a_list": [x.strip() for x in vals[3][1].split(',')],
        "group_b_name": vals[4][1],
        "group_b_list": [x.strip() for x in vals[5][1].split(',')],
        "group_c_name": vals[6][1],
        "group_c_list": [x.strip() for x in vals[7][1].split(',')],
        "mode":         vals[8][1]  # 9行目・B列
    }
    return config

# ==========================================
# BLOCK: 4. クロス分析エンジン
# ==========================================
async def run_analysis():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    gc = gspread.authorize(creds)
    doc = gc.open_by_key(SPREADSHEET_KEY)
    
    conf = get_config(doc)
    print(f"   [指令受理] {conf['store']} の比較分析を開始（モード: {conf['mode']}）")

    raw_ws = doc.worksheet(RAW_DATA_SHEET)
    all_data = raw_ws.get_all_values()
    
    daily_stats = {}

    print("--- 2. 全データをスキャンしてグループ分け中... ---")
    for row in all_data[1:]:
        try:
            d_date, d_store, d_model, d_unit, d_diff, d_games = [c.strip() for c in row]
            if conf['store'] not in d_store: continue
            
            # 数値の抽出
            val = int(d_diff) if conf['mode'] == "差枚" else int(d_games)
            
            if d_date not in daily_stats:
                daily_stats[d_date] = {'all': [], 'A': [], 'B': [], 'C': []}
            
            # A: 店全体
            daily_stats[d_date]['all'].append(val)
            
            # B: 各グループ（部分一致で判定）
            if any(k in d_model for k in conf['group_a_list'] if k):
                daily_stats[d_date]['A'].append(val)
            if any(k in d_model for k in conf['group_b_list'] if k):
                daily_stats[d_date]['B'].append(val)
            if any(k in d_model for k in conf['group_c_list'] if k):
                daily_stats[d_date]['C'].append(val)
        except: continue

    # クロス表の作成
    header = ["日付", "曜日", "店全体平均", conf['group_a_name'], conf['group_b_name'], conf['group_c_name']]
    final_rows = [header]
    
    sorted_dates = sorted(daily_stats.keys(), reverse=False)
    for date_str in sorted_dates:
        dt = datetime.strptime(date_str, "%Y/%m/%d")
        dow = ["月", "火", "水", "木", "金", "土", "日"][dt.weekday()]
        
        def get_avg(lst):
            return int(sum(lst)/len(lst)) if lst else "-"

        day = daily_stats[date_str]
        final_rows.append([
            date_str, dow, get_avg(day['all']), get_avg(day['A']), get_avg(day['B']), get_avg(day['C'])
        ])

    print(f"--- 3. シート '{CROSS_SHEET}' へ反映中... ---")
    try:
        cross_ws = doc.worksheet(CROSS_SHEET)
        doc.del_worksheet(cross_ws)
    except: pass
    cross_ws = doc.add_worksheet(title=CROSS_SHEET, rows=len(final_rows)+50, cols=10)
    cross_ws.update(values=final_rows, range_name='A1')

    # デザイン設定
    cross_ws.format("A1:F1000", {"horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"})
    cross_ws.freeze(rows=1)
    cross_ws.format("A1:F1", {"backgroundColor": hex_to_rgb("#cccccc"), "textFormat": {"bold": True}})

    # 土日色
    for i, row in enumerate(final_rows[1:], start=2):
        if row[1] == "土": cross_ws.format(f"A{i}:B{i}", {"textFormat": {"foregroundColor": {"blue": 1.0}, "bold": True}})
        if row[1] == "日": cross_ws.format(f"A{i}:B{i}", {"textFormat": {"foregroundColor": {"red": 1.0}, "bold": True}})

    print(f"\n【完遂】クロス比較表が完成しました！設定を変えて何度でも実行可能です。")

if __name__ == "__main__":
    asyncio.run(run_analysis())