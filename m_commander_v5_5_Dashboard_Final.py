# --- VERSION: m_commander_v5_5_Dashboard_Final ---
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
# BLOCK: 2. 高度な分析ロジック
# ==========================================
def get_period_rankings(model_data, p_dates):
    """特定期間内のBEST5/WORST5を算出"""
    if not p_dates: return [], []
    p_stats = collections.defaultdict(int)
    for d in p_dates:
        for u, val in model_data[d].items():
            p_stats[u] += val['diff']
    
    # 稼働日数を考慮した「平均差枚」でランキング（PM指定）
    u_averages = []
    for u, total_d in p_stats.items():
        days = len([d for d in p_dates if u in model_data[d]])
        if days > 0:
            u_averages.append((u, int(total_d / days)))
    
    sorted_units = sorted(u_averages, key=lambda x: x[1], reverse=True)
    return sorted_units[:5], sorted_units[-5:][::-1]

def split_periods(model_data, sorted_dates):
    """配置換え検知または3等分で期間を分割"""
    break_points = []
    if not sorted_dates: return []
    
    prev_units = set(model_data[sorted_dates[0]].keys())
    for d in sorted_dates:
        curr_units = set(model_data[d].keys())
        if curr_units != prev_units:
            break_points.append(d)
            prev_units = curr_units
    
    if not break_points:
        n = len(sorted_dates)
        if n >= 3:
            break_points = [sorted_dates[n//3], sorted_dates[2*n//3]]
        else:
            break_points = []

    periods = []
    start_idx = 0
    for bp in break_points + [None]:
        if bp:
            end_idx = sorted_dates.index(bp)
            periods.append(sorted_dates[start_idx:end_idx])
            start_idx = end_idx
        else:
            periods.append(sorted_dates[start_idx:])
    return periods[:3] # 前期・中期・後期

# ==========================================
# BLOCK: 3. メインエンジン
# ==========================================
async def execute_single_analysis(doc, conf):
    print(f"   > 機種別分析: {conf['target_model']} 最終Dashboard構築中...")
    
    # --- STEP 1: データ抽出 (3/5ルール) ---
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
    unit_history = collections.defaultdict(list)
    dow_stats = collections.defaultdict(list)
    digit_stats = collections.defaultdict(list)
    
    # 【追加】店舗全体の集計用
    store_daily_stats = collections.defaultdict(lambda: {'diff': 0, 'games': 0})
    
    payout_history = []
    all_diffs, all_games = [], []

    # 再度CSVをスキャンして店舗全体の期待値を算出
    with open(LOCAL_DATABASE, mode='r', encoding='utf-8-sig') as f:
        reader = csv.reader(f); next(reader, None)
        for row in reader:
            if len(row) < 6: continue
            d_date, d_store, d_model, d_unit, d_diff, d_games = [c.strip() for c in row]
            if conf['store'] not in d_store: continue
            # 店舗全体の累計
            store_daily_stats[d_date]['diff'] += int(d_diff)
            store_daily_stats[d_date]['games'] += int(d_games)

    for entry in raw_data:
        if entry['unit'] in valid_units_all:
            model_data[entry['date']][entry['unit']] = {'diff': entry['diff'], 'games': entry['games']}
            all_diffs.append(entry['diff']); all_games.append(entry['games'])

    sorted_dates = sorted(model_data.keys()); dow_names = ["月", "火", "水", "木", "金", "土", "日"]
    
    # 店舗全体の30日平均機械割を算出
    store_payout_history = []
    for d_str in sorted_dates:
        s_d = store_daily_stats[d_str]['diff']
        s_g = store_daily_stats[d_str]['games']
        s_rate = ((s_g * 3 + s_d) / (s_g * 3) * 100) if s_g > 0 else 100
        store_payout_history.append(s_rate)
    # 期間分割とランキング
    periods = split_periods(model_data, sorted_dates)
    total_best, total_worst = get_period_rankings(model_data, sorted_dates)
    p_results = []
    for p in periods:
        best, worst = get_period_rankings(model_data, p)
        p_results.append({'dates': p, 'best': best, 'worst': worst})

    # --- STEP 3: シート再建築 ---
    try: ws = doc.worksheet(SINGLE_SHEET)
    except WorksheetNotFound: ws = doc.add_worksheet(title=SINGLE_SHEET, rows="2000", cols="200")
    ws.clear(); s_id = ws.id

    # --- STEP 4: Dashboard A1:L31 描画 ---
    # 4-1. ヘッダーサマリー
    avg_diff_total = int(sum(all_diffs)/len(all_diffs)) if all_diffs else 0
    avg_game_total = int(sum(all_games)/len(all_games)) if all_games else 0
    avg_rate_total = ((sum(all_games)*3 + sum(all_diffs))/(sum(all_games)*3)*100) if sum(all_games)>0 else 0
    
    dashboard = [[""] * 12 for _ in range(32)]
    dashboard[0][0], dashboard[0][1] = "【レポート】", conf['store']
    dashboard[1][0], dashboard[1][1] = "機種名▶️", conf['target_model']
    dashboard[1][4], dashboard[1][6], dashboard[1][8] = "🔽TOTAL台平均差枚", "🔽TOTAL台平均G数", "🔽TOTAL機械割"
    dashboard[2][1], dashboard[2][2] = "開始", "最新"
    dashboard[2][4], dashboard[2][6], dashboard[2][8] = f"{avg_diff_total}枚", f"{avg_game_total}G", f"{avg_rate_total:.1f}%"
    dashboard[3][0], dashboard[3][1], dashboard[3][2] = "解析期間▶️", sorted_dates[0], sorted_dates[-1]
    
    dashboard[5][0] = "➖全体分析➖"
    dashboard[6][0], dashboard[6][3], dashboard[6][6] = "🔽曜日分析", "🔽日付末尾分析", "🔽期間分析"
    
    # 曜日・末尾
    for i in range(7):
        dashboard[7+i][0] = dow_names[i]
        dashboard[7+i][1] = int(sum(dow_stats[i])/len(dow_stats[i])) if dow_stats[i] else 0
    for i in range(10):
        dashboard[7+i][3] = f"末{i}"
        dashboard[7+i][4] = int(sum(digit_stats[i])/len(digit_stats[i])) if digit_stats[i] else 0

    # 期間分析 (J, K, L列)
    dashboard[7][7], dashboard[7][9], dashboard[7][10], dashboard[7][11] = "期間", "平均差枚", "機械割", "平均G数"
    for i, p in enumerate(p_results):
        if not p['dates']: continue
        p_label = ["前期", "中期", "後期"][i]
        p_d = [model_data[d] for d in p['dates']]
        p_diff_sum = sum(sum(u['diff'] for u in day.values()) for day in p_d)
        p_game_sum = sum(sum(u['games'] for u in day.values()) for day in p_d)
        p_u_count = sum(len(day) for day in p_d)
        p_avg_d = int(p_diff_sum / p_u_count) if p_u_count > 0 else 0
        p_avg_g = int(p_game_sum / p_u_count) if p_u_count > 0 else 0
        p_rate = ((p_game_sum * 3 + p_diff_sum) / (p_game_sum * 3) * 100) if p_game_sum > 0 else 0
        
        dashboard[8+i][6] = p_label
        dashboard[8+i][7] = f"{p['dates'][0]}〜{p['dates'][-1]}"
        dashboard[8+i][9] = p_avg_d
        dashboard[8+i][10] = f"{p_rate:.1f}%"
        dashboard[8+i][11] = p_avg_g

    # ランキング (18行目〜)
    dashboard[17][0] = "➖個別台分析➖"
    dashboard[18][0], dashboard[18][3], dashboard[18][6], dashboard[18][9] = "TOTAL", "前期", "中期", "後期"
    for col_idx, rank_data in enumerate([ (total_best, total_worst) ] + [ (p['best'], p['worst']) for p in p_results ]):
        if col_idx > 3: break
        c_base = col_idx * 3
        dashboard[19][c_base] = "👑BEST5"
        dashboard[25][c_base] = "💀WORST5"
        for j in range(5):
            if j < len(rank_data[0]):
                dashboard[20+j][c_base], dashboard[20+j][c_base+1] = f"{rank_data[0][j][0]}番台", rank_data[0][j][1]
            if j < len(rank_data[1]):
                dashboard[26+j][c_base], dashboard[26+j][c_base+1] = f"{rank_data[1][j][0]}番台", rank_data[1][j][1]

    ws.update(values=dashboard, range_name='A1')

    # --- STEP 5: データエリア (80行〜) ---
    data_header = ["日付", "曜日", "イベントログ", "総計", "台平均", "平均G", "機械割", "粘り勝率"] + [f"{u}番" for u in valid_units_all]
    data_rows = []
    for i, d_str in enumerate(sorted_dates):
        day_data = model_data[d_str]; u_count = len(day_data)
        t_d, t_g = sum(u['diff'] for u in day_data.values()), sum(u['games'] for u in day_data.values())
        
        # 移動平均
        ma7 = sum(payout_history[max(0, i-6):i+1]) / len(payout_history[max(0, i-6):i+1])
        ma30 = sum(payout_history[max(0, i-29):i+1]) / len(payout_history[max(0, i-29):i+1])
        # 【追加】店舗全体の30日平均
        store_ma30 = sum(store_payout_history[max(0, i-29):i+1]) / len(store_payout_history[max(0, i-29):i+1])

        row = [d_str, dow_names[datetime.strptime(d_str, "%Y/%m/%d").weekday()], "", t_d, int(t_d/u_count), int(t_g/u_count), f"{((t_g*3+t_d)/(t_g*3)*100):.1f}%", f"{(len([u for u in day_data.values() if u['games']>=5000 and u['diff']>0])/u_count*100):.1f}%"]
        for u in valid_units_all: row.append(day_data[u]['diff'] if u in day_data else "")
        # グラフ用：機種MA7, 機種MA30, 店舗MA30
        row += ["", ma7, ma30, store_ma30]
        data_rows.append(row)

    # --- STEP 6: パノラマグラフ ＆ 書式 ---
    meta = doc.fetch_sheet_metadata(); target_sheet = next(s for s in meta['sheets'] if s['properties']['sheetId'] == s_id)
    charts = target_sheet.get('charts', [])
    reqs = []
    if charts:
        for c in charts: reqs.append({"deleteEmbeddedObject": {"objectId": c['chartId']}})
    
    # --- STEP 6: パノラマグラフ ＆ 書式 ---
    # グラフ軸 95-110
    l_row, l_col = len(data_rows) + 80, len(data_header)
    reqs.append({"addChart": {"chart": {"spec": {"title": "トレンド比較 (青:MA7 赤:MA30 灰:店全体30MA)", "basicChart": {"chartType": "LINE", "legendPosition": "BOTTOM_LEGEND", 
        "axis": [{"position": "BOTTOM_AXIS"}, {"position": "LEFT_AXIS", "viewWindowOptions": {"viewWindowMin": 95, "viewWindowMax": 110, "viewWindowMode": "EXPLICIT"}}],
        "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 79, "endRowIndex": l_row, "startColumnIndex": 0, "endColumnIndex": 1}]}}}],
        "series": [
            {"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 79, "endRowIndex": l_row, "startColumnIndex": l_col+1, "endColumnIndex": l_col+2}]}}, "color": {"blue": 1.0}, "lineStyle": {"width": 2}}, # 機種MA7
            {"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 79, "endRowIndex": l_row, "startColumnIndex": l_col+2, "endColumnIndex": l_col+3}]}}, "color": {"red": 1.0}, "lineStyle": {"width": 3}}, # 機種MA30
            {"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 79, "endRowIndex": l_row, "startColumnIndex": l_col+3, "endColumnIndex": l_col+4}]}}, "color": {"red": 0.8, "green": 0.8, "blue": 0.8}, "lineStyle": {"width": 6}} # 店舗MA30(太い薄グレー)
        ]}},
        "position": {"overlayPosition": {"anchorCell": {"sheetId": s_id, "rowIndex": 34, "columnIndex": 0}, "widthPixels": 3200, "heightPixels": 450}}}}})

    # 列幅 60px / 祝日判定
    reqs.append({"updateDimensionProperties": {"range": {"sheetId": s_id, "dimension": "COLUMNS", "startIndex": 3, "endIndex": l_col}, "properties": {"pixelSize": 60}, "fields": "pixelSize"}})
    for i, d_str in enumerate(sorted_dates):
        dt = datetime.strptime(d_str, "%Y/%m/%d")
        color = {"red": 1, "green": 0, "blue": 0} if dt.weekday()==6 or jpholiday.is_holiday(dt) else ({"red": 0, "green": 0, "blue": 1} if dt.weekday()==5 else {"red": 0, "green": 0, "blue": 0})
        reqs.append({"updateCells": {"range": {"sheetId": s_id, "startRowIndex": 79+i, "endRowIndex": 80+i, "startColumnIndex": 0, "endColumnIndex": 2}, "rows": [{"values": [{"userEnteredFormat": {"textFormat": {"foregroundColor": color}}}, {"userEnteredFormat": {"textFormat": {"foregroundColor": color}}}]}], "fields": "userEnteredFormat.textFormat.foregroundColor"}})

    doc.batch_update({"requests": reqs})
    print("\n   -> Version 5.5 完成。")

async def main():
    print(f"\n--- Ver.5.5 起動 (Final Dashboard) ---")
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
    gc = gspread.authorize(creds); doc = gc.open_by_key(SPREADSHEET_KEY)
    await sync_store_list(doc)
    while True:
        try:
            conf_ws = doc.worksheet(CONFIG_SHEET); vals = conf_ws.get_all_values()
            if "実行" in str([vals[1][1], vals[7][2]]):
                btn = 'B2' if "実行" in vals[1][1] else 'C8'
                conf_ws.update_acell(btn, "● 実行中")
                await execute_single_analysis(doc, {"store": vals[4][1], "target_model": vals[7][1]})
                conf_ws.update_acell(btn, "待機中")
            print(f"\r[{datetime.now().strftime('%H:%M:%S')}] STAND BY ...", end="")
        except Exception as e: print(f"\nError: {e}")
        await asyncio.sleep(15)

if __name__ == "__main__": asyncio.run(main())