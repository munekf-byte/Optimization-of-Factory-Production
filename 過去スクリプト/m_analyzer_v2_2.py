# --- VERSION: m_analyzer_v2.2_Mobile_Ready_20260129 ---

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import asyncio

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

def calculate_machine_rate(total_diff, total_games):
    if total_games == 0: return 0
    return round(((total_games * 3) + total_diff) / (total_games * 3) * 100, 2)

# ==========================================
# BLOCK: 3. 設定読み込み（10枠対応版）
# ==========================================
def get_config_v2(doc):
    print("--- 1. 司令塔から10枠の指示を読み込み中... ---")
    conf_ws = doc.worksheet(CONFIG_SHEET)
    vals = conf_ws.get_all_values()
    
    # セル番地に基づいた読み取り
    config = {
        "store": vals[1][1],
        "mode":  vals[2][1],
        "A": [v[1] for v in vals[4:14] if v[1]],  # 5行目〜14行目
        "B": [v[1] for v in vals[15:25] if v[1]], # 16行目〜25行目
        "C": [v[1] for v in vals[26:36] if v[1]]  # 27行目〜36行目
    }
    return config

# ==========================================
# BLOCK: 4. クロス分析エンジン（機械割対応）
# ==========================================
async def run_analysis():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    gc = gspread.authorize(creds)
    doc = gc.open_by_key(SPREADSHEET_KEY)
    
    conf = get_config_v2(doc)
    print(f"   [指令受理] {conf['store']} の比較分析を開始（モード: {conf['mode']}）")

    raw_ws = doc.worksheet(RAW_DATA_SHEET)
    all_data = raw_ws.get_all_values()
    
    daily_stats = {} # {日付: { 'all': {diff, g}, 'A': {diff, g} ... }}

    print("--- 2. 全データをスキャンして部門分け中... ---")
    for row in all_data[1:]:
        try:
            d_date, d_store, d_model, d_unit, d_diff, d_games = [c.strip() for c in row]
            if conf['store'] not in d_store: continue
            
            diff, games = int(d_diff), int(d_games)
            
            if d_date not in daily_stats:
                daily_stats[d_date] = {'all': [], 'A': [], 'B': [], 'C': []}
            
            # 生データ形式を辞書に
            entry = {'diff': diff, 'games': games}
            daily_stats[d_date]['all'].append(entry)
            
            if d_model in conf['A']: daily_stats[d_date]['A'].append(entry)
            if d_model in conf['B']: daily_stats[d_date]['B'].append(entry)
            if d_model in conf['C']: daily_stats[d_date]['C'].append(entry)
        except: continue

    # 表の作成
    header = ["日付", "曜日", "店全体", "部門A", "部門B", "部門C"]
    final_rows = [header]
    
    sorted_dates = sorted(daily_stats.keys(), reverse=False)
    for date_str in sorted_dates:
        dt = datetime.strptime(date_str, "%Y/%m/%d")
        dow = ["月", "火", "水", "木", "金", "土", "日"][dt.weekday()]
        
        def calculate_value(entries):
            if not entries: return "-"
            if conf['mode'] == "差枚":
                return int(sum(e['diff'] for e in entries) / len(entries))
            elif conf['mode'] == "G数":
                return int(sum(e['games'] for e in entries) / len(entries))
            elif conf['mode'] == "機械割":
                t_diff = sum(e['diff'] for e in entries)
                t_games = sum(e['games'] for e in entries)
                return f"{calculate_machine_rate(t_diff, t_games)}%"
            return "-"

        day = daily_stats[date_str]
        final_rows.append([
            date_str, dow, 
            calculate_value(day['all']), 
            calculate_value(day['A']), 
            calculate_value(day['B']), 
            calculate_value(day['C'])
        ])

    print(f"--- 3. シート '{CROSS_SHEET}' へ反映中... ---")
    try:
        cross_ws = doc.worksheet(CROSS_SHEET)
        doc.del_worksheet(cross_ws)
    except: pass
    cross_ws = doc.add_worksheet(title=CROSS_SHEET, rows=len(final_rows)+50, cols=10)
    cross_ws.update(values=final_rows, range_name='A1')

    # デザイン
    cross_ws.format("A1:F1000", {"horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"})
    cross_ws.freeze(rows=1)
    cross_ws.format("A1:F1", {"backgroundColor": hex_to_rgb("#cccccc"), "textFormat": {"bold": True}})

    print(f"\n【完遂】クロス比較表（Ver.2.2）が完成しました！")

if __name__ == "__main__":
    asyncio.run(run_analysis())