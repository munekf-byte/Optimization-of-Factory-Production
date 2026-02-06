# --- VERSION: m_commander_v5_3_Panorama_Intelligence ---
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
def get_period_stats(model_data, sorted_dates, valid_units):
    """期間を最大3分割(前・中・後)し、ランキングを算出する"""
    # 配置換え検知（簡易版：全台数や構成の変化）
    break_points = []
    prev_units = set(model_data[sorted_dates[0]].keys())
    for d in sorted_dates:
        curr_units = set(model_data[d].keys())
        if curr_units != prev_units:
            break_points.append(d)
            prev_units = curr_units
    
    # 期間の切り出し
    if not break_points: # 配置換えがない場合は均等3分割
        n = len(sorted_dates)
        break_points = [sorted_dates[n//3], sorted_dates[2*n//3]]
    
    periods = []
    start_idx = 0
    for bp in break_points + [None]:
        if bp:
            end_idx = sorted_dates.index(bp)
            periods.append(sorted_dates[start_idx:end_idx])
            start_idx = end_idx
        else:
            periods.append(sorted_dates[start_idx:])
    
    results = []
    for i, p_dates in enumerate(periods[:3]): # 最大3期
        if not p_dates: continue
        label = ["前期", "中期", "後期"][i]
        p_stats = collections.defaultdict(int)
        for d in p_dates:
            for u, val in model_data[d].items():
                p_stats[u] += val['diff']
        
        # ソートしてTOP10/WORST10
        sorted_units = sorted(p_stats.items(), key=lambda x: x[1], reverse=True)
        results.append({
            'label': label,
            'range': f"{p_dates[0]}〜{p_dates[-1]}",
            'top10': sorted_units[:10],
            'worst10': sorted_units[-10:][::-1]
        })
    return results

# ==========================================
# BLOCK: 3. メイン分析エンジン
# ==========================================
async def execute_single_analysis(doc, conf):
    print(f"   > 機種別分析: {conf['target_model']} 高度解析中...")
    
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
    dow_stats = collections.defaultdict(list)
    end_digit_stats = collections.defaultdict(list)
    payout_history = []

    for entry in raw_data:
        if entry['unit'] in valid_units_all:
            model_data[entry['date']][entry['unit']] = {'diff': entry['diff'], 'games': entry['games']}
    
    sorted_dates = sorted(model_data.keys()); dow_names = ["月", "火", "水", "木", "金", "土", "日"]
    
    for i, d_str in enumerate(sorted_dates):
        dt = datetime.strptime(d_str, "%Y/%m/%d")
        day_data = model_data[d_str]
        t_d, t_g = sum(u['diff'] for u in day_data.values()), sum(u['games'] for u in day_data.values())
        m_rate = ((t_g * 3 + t_d) / (t_g * 3) * 100) if t_g > 0 else 100
        payout_history.append(m_rate)
        
        dow_stats[dt.weekday()].append(t_d)
        end_digit_stats[dt.day % 10].append(t_d)

    # 三期ランキング取得
    period_rankings = get_period_stats(model_data, sorted_dates, valid_units_all)

    # --- STEP 3: レポートエリア構築 (1-79行目) ---
    ws = doc.worksheet(SINGLE_SHEET); ws.clear()
    
    # 3-1. 曜日・末尾分析 (横並び)
    ws.update(values=[["【曜日別分析】"] + dow_names, ["平均差枚"] + [int(sum(dow_stats[i])/len(dow_stats[i])) if dow_stats[i] else 0 for i in range(7)]], range_name='A1')
    ws.update(values=[["【末尾分析】"] + [f"末{i}" for i in range(10)], ["平均差枚"] + [int(sum(end_digit_stats[i])/len(end_digit_stats[i])) if end_digit_stats[i] else 0 for i in range(10)]], range_name='A4')
    
    # 3-2. 三期ランキング (A, D, G列で衝突回避)
    for i, p in enumerate(period_rankings):
        col_start = ["A", "D", "G"][i]
        rows = [[f"【{p['label']}ランキング】", p['range']], ["優秀台TOP10", "差枚", "地雷台Worst10", "差枚"]]
        for j in range(10):
            t_u = f"{p['top10'][j][0]}番" if j < len(p['top10']) else "-"
            t_v = p['top10'][j][1] if j < len(p['top10']) else "-"
            w_u = f"{p['worst10'][j][0]}番" if j < len(p['worst10']) else "-"
            w_v = p['worst10'][j][1] if j < len(p['worst10']) else "-"
            rows.append([t_u, t_v, w_u, w_v])
        ws.update(values=rows, range_name=f'{col_start}8')

    # --- STEP 4: データテーブル構築 (80行目〜) ---
    data_header = ["日付", "曜日", "イベントログ", "総計", "台平均", "平均G", "機械割", "粘り勝率"] + [f"{u}番" for u in valid_units_all]
    data_rows = []
    for i, d_str in enumerate(sorted_dates):
        day_data = model_data[d_str]; u_count = len(day_data)
        t_d, t_g = sum(u['diff'] for u in day_data.values()), sum(u['games'] for u in day_data.values())
        m_rate = ((t_g * 3 + t_d) / (t_g * 3)) * 100 if t_g > 0 else 100
        sticky = (len([u for u in day_data.values() if u['games']>=5000 and u['diff']>0])/u_count) * 100
        ma7 = sum(payout_history[max(0, i-6):i+1]) / len(payout_history[max(0, i-6):i+1])
        ma30 = sum(payout_history[max(0, i-29):i+1]) / len(payout_history[max(0, i-29):i+1])

        row = [d_str, dow_names[datetime.strptime(d_str, "%Y/%m/%d").weekday()], "", t_d, int(t_d/u_count), int(t_g/u_count), f"{m_rate:.1f}%", f"{sticky:.1f}%"]
        for u in valid_units_all: row.append(day_data[u]['diff'] if u in day_data else "")
        row += ["", ma7, ma30] # グラフ用隠しデータ
        data_rows.append(row)
    
    ws.update(values=[data_header] + data_rows, range_name='A80')

    # --- STEP 5: グラフ削除 ＆ パノラマグラフ追加 ---
    s_id = ws.id; l_row = len(data_rows) + 80; l_col = len(data_header)
    meta = doc.fetch_sheet_metadata(); target_sheet = next(s for s in meta['sheets'] if s['properties']['sheetId'] == s_id)
    charts = target_sheet.get('charts', [])
    
    reqs = []
    if charts: # グラフの亡霊を削除
        for c in charts: reqs.append({"deleteEmbeddedObject": {"objectId": c['chartId']}})
    
    # 列幅の黄金比
    reqs.append({"updateDimensionProperties": {"range": {"sheetId": s_id, "dimension": "COLUMNS", "startIndex": 3, "endIndex": l_col}, "properties": {"pixelSize": 60}, "fields": "pixelSize"}})
    reqs.append({"updateDimensionProperties": {"range": {"sheetId": s_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 2}, "properties": {"pixelSize": 90}, "fields": "pixelSize"}})

    # 超ワイド・パノラマグラフ
    reqs.append({"addChart": {"chart": {"spec": {"title": "機種トレンド (機械割 MA7 vs MA30)", "basicChart": {"chartType": "LINE", "legendPosition": "BOTTOM_LEGEND", 
        "axis": [{"position": "BOTTOM_AXIS"}, {"position": "LEFT_AXIS", "viewWindowMode": "EXPLICIT", "viewWindow": {"min": 80, "max": 110}}],
        "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 79, "endRowIndex": l_row, "startColumnIndex": 0, "endColumnIndex": 1}]}}}],
        "series": [
            {"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 79, "endRowIndex": l_row, "startColumnIndex": l_col+1, "endColumnIndex": l_col+2}]}}, "targetAxis": "LEFT_AXIS", "color": {"red": 0.2, "green": 0.2, "blue": 1.0}, "lineStyle": {"width": 2}},
            {"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 79, "endRowIndex": l_row, "startColumnIndex": l_col+2, "endColumnIndex": l_col+3}]}}, "targetAxis": "LEFT_AXIS", "color": {"red": 1.0, "green": 0.2, "blue": 0.2}, "lineStyle": {"width": 3}}
        ]}}, "position": {"overlayPosition": {"anchorCell": {"sheetId": s_id, "rowIndex": 0, "columnIndex": 12}, "widthPixels": 3200, "heightPixels": 450}}}}})

    doc.batch_update({"requests": reqs})
    print("\n   -> 機種別分析 v5.3 完了 (Panorama & Triple Period Active)")

async def main():
    print(f"\n--- Ver.5.3 起動 (Panorama & Period Analytics) ---")
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