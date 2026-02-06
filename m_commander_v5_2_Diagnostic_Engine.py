# --- VERSION: m_commander_v5_2_Diagnostic_Engine ---
import gspread
from gspread.exceptions import WorksheetNotFound
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import asyncio
import csv
import collections
import jpholiday

# ==========================================
# BLOCK: 1. 固定設定
# ==========================================
SPREADSHEET_KEY = "1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I"
CONFIG_SHEET    = "分析設定"
SINGLE_SHEET    = "機種別分析"
INDEX_SHEET     = "機種目録" 
LOCAL_DATABASE  = "/Users/macuser/Desktop/minrepo_project/minrepo_database.csv"

# ==========================================
# BLOCK: 2. 診断・ロジック補助
# ==========================================
def detect_relocation(history, sorted_dates):
    """台番号セットの変化を検知し、施策を診断する"""
    diagnostics = []
    prev_units = None
    
    # 期間全体のパフォーマンス計算用
    def get_perf(start_dt, end_dt, data_dict):
        diffs, games = [], []
        curr = start_dt
        while curr <= end_dt:
            d_str = curr.strftime("%Y/%m/%d")
            if d_str in data_dict:
                diffs += [u['diff'] for u in data_dict[d_str].values()]
                games += [u['games'] for u in data_dict[d_str].values()]
            curr += timedelta(days=1)
        avg_g = sum(games)/len(games) if games else 0
        avg_d = sum(diffs)/len(diffs) if diffs else 0
        return avg_g, avg_d

    unit_sets = {}
    for d in sorted_dates:
        unit_sets[d] = set(history[d].keys())

    for i in range(1, len(sorted_dates)):
        d_curr = sorted_dates[i]
        d_prev = sorted_dates[i-1]
        set_curr = unit_sets[d_curr]
        set_prev = unit_sets[d_prev]

        if set_curr != set_prev:
            # 変化発生
            event_type = "引越し"
            if len(set_curr) > len(set_prev): event_type = "増台"
            elif len(set_curr) < len(set_prev): event_type = "減台"
            
            # 前後7日の比較診断
            dt_event = datetime.strptime(d_curr, "%Y/%m/%d")
            before_g, before_d = get_perf(dt_event - timedelta(days=8), dt_event - timedelta(days=1), history)
            after_g, after_d = get_perf(dt_event, dt_event + timedelta(days=7), history)
            
            res = "成功" if after_g > before_g else "失敗"
            diag = f"【{d_curr}】{event_type}検知: G数 {int(before_g)}→{int(after_g)} ({res})"
            if event_type == "増台" and after_d > before_d: diag += " / 強化傾向"
            diagnostics.append(diag)
            
    return diagnostics[:5] # 最新5件まで表示

# ==========================================
# BLOCK: 3. 機種別分析（v5.2 診断エンジン搭載）
# ==========================================
async def execute_single_analysis(doc, conf):
    print(f"   > 機種別分析: {conf['target_model']} 診断中...")
    
    # --- STEP 1: データ抽出 ---
    unit_appearance = collections.defaultdict(list); raw_data = []
    with open(LOCAL_DATABASE, mode='r', encoding='utf-8-sig') as f:
        reader = csv.reader(f); next(reader, None)
        for row in reader:
            if len(row) < 6: continue
            d_date, d_store, d_model, d_unit, d_diff, d_games = [c.strip() for c in row]
            if not d_unit.isdigit() or conf['store'] not in d_store or conf['target_model'] not in d_model: continue
            dt = datetime.strptime(d_date, "%Y/%m/%d")
            unit_appearance[int(d_unit)].append(dt)
            raw_data.append({'date': d_date, 'unit': int(d_unit), 'diff': int(d_diff), 'games': int(d_games)})

    valid_units_all = sorted([u for u, dates in unit_appearance.items() if any((sorted(dates)[i+2] - sorted(dates)[i]).days <= 4 for i in range(len(dates)-2))])
    if not valid_units_all: return

    # --- STEP 2: 集計 ---
    model_data = collections.defaultdict(dict)
    for entry in raw_data:
        if entry['unit'] in valid_units_all:
            model_data[entry['date']][entry['unit']] = {'diff': entry['diff'], 'games': entry['games']}
    
    sorted_dates = sorted(model_data.keys()); dow_names = ["月", "火", "水", "木", "金", "土", "日"]
    
    # 配置診断の実行
    diag_results = detect_relocation(model_data, sorted_dates)

    # 月末・月中・月初集計
    term_stats = {"月初(1-10)": [], "月中(11-20)": [], "月末(21-)": []}
    payout_history = [] # グラフ用
    
    for d_str in sorted_dates:
        dt = datetime.strptime(d_str, "%Y/%m/%d")
        day_data = model_data[d_str]
        t_d, t_g = sum(u['diff'] for u in day_data.values()), sum(u['games'] for u in day_data.values())
        m_rate = ((t_g * 3 + t_d) / (t_g * 3) * 100) if t_g > 0 else 100
        payout_history.append(m_rate)
        
        day_val = dt.day
        key = "月初(1-10)" if day_val <= 10 else ("月中(11-20)" if day_val <= 20 else "月末(21-)")
        term_stats[key].append(m_rate)

    # 3-1. レポート上部
    report_top = [["【診断レポート】", conf['store'], conf['target_model']], ["最新の配置診断:"]]
    for r in diag_results: report_top.append([r])
    
    # 3-2. 区分別期待値
    term_header = ["時期区分", "月初(1-10)", "月中(11-20)", "月末(21-)"]
    term_values = ["平均機械割"] + [f"{sum(term_stats[k])/len(term_stats[k]):.1f}%" if term_stats[k] else "-" for k in ["月初(1-10)", "月中(11-20)", "月末(21-)"]]

    # 4. データエリア構築
    data_header = ["日付", "曜日", "イベントログ", "総計", "台平均", "平均G", "機械割", "粘り勝率"] + [f"{u}番" for u in valid_units_all]
    data_rows = []
    for i, d_str in enumerate(sorted_dates):
        day_data = model_data[d_str]; u_count = len(day_data)
        t_d, t_g = sum(u['diff'] for u in day_data.values()), sum(u['games'] for u in day_data.values())
        m_rate = ((t_g * 3 + t_d) / (t_g * 3)) * 100 if t_g > 0 else 100
        sticky = (len([u for u in day_data.values() if u['games']>=5000 and u['diff']>0])/u_count) * 100
        
        # 移動平均計算 (7日/30日)
        ma7 = sum(payout_history[max(0, i-6):i+1]) / len(payout_history[max(0, i-6):i+1])
        ma30 = sum(payout_history[max(0, i-29):i+1]) / len(payout_history[max(0, i-29):i+1])

        row = [d_str, dow_names[datetime.strptime(d_str, "%Y/%m/%d").weekday()], "", t_d, int(t_d/u_count), int(t_g/u_count), f"{m_rate:.1f}%", f"{sticky:.1f}%"]
        for u in valid_units_all: row.append(day_data[u]['diff'] if u in day_data else "")
        # グラフ用隠しデータ (U, V, W列付近に配置)
        row += ["", ma7, ma30] 
        data_rows.append(row)

    # --- STEP 5: シート建築 & 黄金比フォーマット ---
    try: ws = doc.worksheet(SINGLE_SHEET)
    except WorksheetNotFound: ws = doc.add_worksheet(title=SINGLE_SHEET, rows="2000", cols="200")
    
    ws.clear()
    ws.update(values=report_top, range_name='A1')
    ws.update(values=[term_header, term_values], range_name='A10')
    ws.update(values=[data_header] + data_rows, range_name='A20')
    
    # 黄金比の列幅固定 (60px)
    s_id = ws.id; last_col = len(data_header)
    reqs = [
        {"updateDimensionProperties": {"range": {"sheetId": s_id, "dimension": "COLUMNS", "startIndex": 3, "endIndex": last_col}, "properties": {"pixelSize": 60}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": s_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1}, "properties": {"pixelSize": 90}, "fields": "pixelSize"}}
    ]
    
    # トレンドグラフ追加 (機械割 7MA vs 30MA)
    chart_row_len = len(data_rows)
    reqs.append({"addChart": {"chart": {"spec": {"title": "機種トレンド (機械割 MA7 vs MA30)", "basicChart": {"chartType": "LINE", "legendPosition": "BOTTOM_LEGEND", "axis": [{"position": "BOTTOM_AXIS"}, {"position": "LEFT_AXIS"}], 
        "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 19, "endRowIndex": 20+chart_row_len, "startColumnIndex": 0, "endColumnIndex": 1}]}}}],
        "series": [
            {"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 19, "endRowIndex": 20+chart_row_len, "startColumnIndex": last_col+1, "endColumnIndex": last_col+2}]}}, "targetAxis": "LEFT_AXIS", "color": {"red": 0.2, "green": 0.2, "blue": 1.0}, "lineStyle": {"width": 2}},
            {"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 19, "endRowIndex": 20+chart_row_len, "startColumnIndex": last_col+2, "endColumnIndex": last_col+3}]}}, "targetAxis": "LEFT_AXIS", "color": {"red": 1.0, "green": 0.2, "blue": 0.2}, "lineStyle": {"width": 3}}
        ]}}, "position": {"overlayPosition": {"anchorCell": {"sheetId": s_id, "rowIndex": 0, "columnIndex": 6}, "widthPixels": 800, "heightPixels": 350}}}}})

    doc.batch_update({"requests": reqs})
    print("\n   -> 機種別分析 v5.2 完了 (Diagnostic Engine Active)")

async def main():
    print(f"\n--- Ver.5.2 起動 (Diagnostic Engine) ---")
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
    gc = gspread.authorize(creds); doc = gc.open_by_key(SPREADSHEET_KEY)
    while True:
        try:
            conf_ws = doc.worksheet(CONFIG_SHEET); vals = conf_ws.get_all_values()
            if "実行" in str([vals[1][1], vals[7][2]]):
                cell = 'B2' if "実行" in vals[1][1] else 'C8'
                conf_ws.update_acell(cell, "● 実行中")
                await execute_single_analysis(doc, {"store": vals[4][1], "target_model": vals[7][1]})
                conf_ws.update_acell(cell, "待機中")
            print(f"\r[{datetime.now().strftime('%H:%M:%S')}] STAND BY ...", end="")
        except Exception as e: print(f"\nError: {e}")
        await asyncio.sleep(15)

if __name__ == "__main__": asyncio.run(main())