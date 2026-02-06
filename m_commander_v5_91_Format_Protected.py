# --- VERSION: m_commander_v5_91_Format_Protected ---
# 変更点: 
# 1. ws.clear() -> batch_clear(['A1:O33', 'A80:Z2000']) に変更（書式を維持し値のみ消去）
# 2. 列幅強制リセット(updateDimensionProperties)をコメントアウト（PMの列幅設定を優先）

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
# BLOCK: 2. 同期エンジン
# ==========================================
async def sync_store_list(doc):
    try:
        unique_stores = set()
        with open(LOCAL_DATABASE, mode='r', encoding='utf-8-sig') as f:
            reader = csv.reader(f); next(reader, None) 
            for row in reader:
                if len(row) > 1: unique_stores.add(row[1])
        stores = sorted(list(unique_stores))
        idx_ws = doc.worksheet(INDEX_SHEET)
        idx_ws.clear()
        idx_ws.update(values=[["店舗リスト(AutoSync)"]] + [[s] for s in stores], range_name='A1')
        print(f"   -> 店舗同期完了: {len(stores)}店舗。")
    except Exception as e: print(f"   ! 同期エラー: {e}")

# ==========================================
# BLOCK: 3. 高度な分析ロジック
# ==========================================
def get_period_rankings_5(model_data, p_dates):
    if not p_dates: return [], []
    p_stats = collections.defaultdict(int)
    for d in p_dates:
        if d in model_data:
            for u, val in model_data[d].items(): p_stats[u] += val['diff']
    u_avgs = []
    for u, total_d in p_stats.items():
        active_days = len([d for d in p_dates if d in model_data and u in model_data[d]])
        if active_days > 0: u_avgs.append((u, int(total_d / active_days)))
    sorted_units = sorted(u_avgs, key=lambda x: x[1], reverse=True)
    return sorted_units[:5], sorted_units[-5:][::-1]

def split_periods_3(model_data, sorted_dates):
    if not sorted_dates: return []
    first_date = sorted_dates[0]
    prev_units = set(model_data[first_date].keys()) if first_date in model_data else set()
    break_points = []
    for d in sorted_dates:
        curr_units = set(model_data[d].keys()) if d in model_data else set()
        if curr_units and curr_units != prev_units:
            break_points.append(d)
            prev_units = curr_units
    if not break_points:
        n = len(sorted_dates)
        if n >= 3: break_points = [sorted_dates[n//3], sorted_dates[2*n//3]]
    periods, start_idx = [], 0
    for bp in break_points + [None]:
        if bp:
            end_idx = sorted_dates.index(bp)
            periods.append(sorted_dates[start_idx:end_idx]); start_idx = end_idx
        else:
            periods.append(sorted_dates[start_idx:])
    return [p for p in periods if p][:3]

# ==========================================
# BLOCK: 4. メイン分析エンジン
# ==========================================
async def execute_single_analysis(doc, conf):
    print(f"   > 機種別分析: {conf['target_model']} 解析中...")
    dow_names = ["月", "火", "水", "木", "金", "土", "日"]
    
    unit_appearance, raw_data = collections.defaultdict(list), []
    store_daily_stats = collections.defaultdict(lambda: {'diff': 0, 'games': 0})
    
    with open(LOCAL_DATABASE, mode='r', encoding='utf-8-sig') as f:
        reader = csv.reader(f); next(reader, None)
        for row in reader:
            if len(row) < 6: continue
            d_date, d_store, d_model, d_unit, d_diff, d_games = [c.strip() for c in row]
            if conf['store'] not in d_store: continue
            store_daily_stats[d_date]['diff'] += int(d_diff)
            store_daily_stats[d_date]['games'] += int(d_games)
            if conf['target_model'] in d_model:
                dt = datetime.strptime(d_date, "%Y/%m/%d")
                unit_appearance[int(d_unit)].append(dt)
                raw_data.append({'date': d_date, 'unit': int(d_unit), 'diff': int(d_diff), 'games': int(d_games)})

    valid_units = sorted([u for u, dates in unit_appearance.items() if any((sorted(dates)[i+2] - sorted(dates)[i]).days <= 4 for i in range(len(dates)-2))])
    if not valid_units: return

    model_data, unit_history = collections.defaultdict(dict), collections.defaultdict(list)
    dow_stats, digit_stats = collections.defaultdict(list), collections.defaultdict(list)
    payout_h, store_payout_h = [], []
    all_diffs, all_games = [], []

    target_dates = sorted(list(set(r['date'] for r in raw_data)))
    for d_str in target_dates:
        day_units = [r for r in raw_data if r['date'] == d_str and r['unit'] in valid_units]
        t_d, t_g = sum(r['diff'] for r in day_units), sum(r['games'] for r in day_units)
        payout_h.append(((t_g*3+t_d)/(t_g*3)*100) if t_g > 0 else 100)
        for r in day_units:
            model_data[d_str][r['unit']] = {'diff': r['diff'], 'games': r['games']}
            unit_history[r['unit']].append(r['diff'])
            all_diffs.append(r['diff']); all_games.append(r['games'])
        s_d, s_g = store_daily_stats[d_str]['diff'], store_daily_stats[d_str]['games']
        store_payout_h.append(((s_g*3+s_d)/(s_g*3)*100) if s_g > 0 else 100)
        dt = datetime.strptime(d_str, "%Y/%m/%d")
        if day_units:
            avg_d = t_d / len(day_units)
            dow_stats[dt.weekday()].append(avg_d); digit_stats[dt.day % 10].append(avg_d)

    periods = split_periods_3(model_data, target_dates)
    total_best, total_worst = get_period_rankings_5(model_data, target_dates)
    p_res = [{'dates': p, 'best': get_period_rankings_5(model_data, p)[0], 'worst': get_period_rankings_5(model_data, p)[1]} for p in periods]

    # --- STEP 3 & 4: シート更新（書式維持モード） ---
    try: 
        ws = doc.worksheet(SINGLE_SHEET)
        # 値のみをクリア（書式は残す）
        ws.batch_clear(['A1:O33', 'A80:Z2000'])
    except WorksheetNotFound: 
        ws = doc.add_worksheet(title=SINGLE_SHEET, rows="2000", cols="200")
    
    s_id = ws.id

    avg_d_total, avg_g_total = int(sum(all_diffs)/len(all_diffs)) if all_diffs else 0, int(sum(all_games)/len(all_games)) if all_games else 0
    avg_r_total = ((sum(all_games)*3+sum(all_diffs))/(sum(all_games)*3)*100) if sum(all_games)>0 else 0
    
    dash = [[""] * 15 for _ in range(33)]
    dash[0][0], dash[0][1] = "【レポート】", conf['store']
    dash[1][0], dash[1][1] = "機種名▶️", conf['target_model']
    dash[1][6], dash[1][9], dash[1][12] = "🔽TOTAL台平均差枚", "🔽TOTAL台平均G数", "🔽TOTAL機械割"
    dash[2][6], dash[2][9], dash[2][12] = f"{avg_d_total}枚", f"{avg_g_total}G", f"{avg_r_total:.1f}%"
    dash[3][0], dash[3][1], dash[3][3] = "解析期間▶️", target_dates[0], target_dates[-1]
    dash[5][0] = "➖全体分析➖"
    dash[6][0], dash[6][3], dash[6][6] = "♦️曜日分析♦️", "♠️日付末尾分析♠️", "♣️期間別分析♣️"
    
    for i in range(7): dash[7+i][0], dash[7+i][1] = dow_names[i], f"{int(sum(dow_stats[i])/len(dow_stats[i]))}枚" if dow_stats[i] else "0枚"
    for i in range(10): dash[7+i][3], dash[7+i][4] = f"末{i}", f"{int(sum(digit_stats[i])/len(digit_stats[i]))}枚" if digit_stats[i] else "0枚"
    
    dash[7][7], dash[7][10], dash[7][11], dash[7][12] = "＜期間＞", "＜差枚＞", "＜割＞", "＜G数＞"
    for i, p in enumerate(p_res):
        p_d = [model_data[d] for d in p['dates'] if d in model_data]
        if not p_d: continue
        p_ds, p_gs, p_uc = sum(sum(u['diff'] for u in day.values()) for day in p_d), sum(sum(u['games'] for u in day.values()) for day in p_d), sum(len(day) for day in p_d)
        dash[8+i][6], dash[8+i][7] = ["前期","中期","後期"][i], f"{p['dates'][0]}〜{p['dates'][-1]}"
        dash[8+i][10], dash[8+i][11], dash[8+i][12] = f"{int(p_ds/p_uc)}枚", f"{(p_gs*3+p_ds)/(p_gs*3)*100:.1f}%", f"{int(p_gs/p_uc)}G"

    dash[18][0], dash[19][1], dash[19][4], dash[19][7], dash[19][10] = "➖個別台分析➖", "全期間", "前期", "中期", "後期"
    for col_idx, r in enumerate([(total_best, total_worst)] + [(p['best'], p['worst']) for p in p_res]):
        if col_idx > 3: break
        c_b = col_idx * 3
        dash[20][c_b], dash[26][c_b] = "👑BEST5", "💀WORST5"
        for j in range(5):
            if j < len(r[0]): dash[21+j][c_b], dash[21+j][c_b+1] = f"{r[0][j][0]}番台", f"{r[0][j][1]}枚"
            if j < len(r[1]): dash[27+j][c_b], dash[27+j][c_b+1] = f"{r[1][j][0]}番台", f"{r[1][j][1]}枚"
    ws.update(values=dash, range_name='A1')

    # --- STEP 5: データ倉庫 ---
    data_header = ["日付", "曜日", "イベントログ", "総計", "台平均", "平均G", "機械割", "粘り勝率"] + [f"{u}番" for u in valid_units]
    data_rows = []
    for i, d_str in enumerate(target_dates):
        day_data = model_data[d_str]; u_cnt = len(day_data)
        if u_cnt == 0: continue
        t_d, t_g = sum(u['diff'] for u in day_data.values()), sum(u['games'] for u in day_data.values())
        ma7, ma30, s_ma30 = sum(payout_h[max(0, i-6):i+1])/len(payout_h[max(0, i-6):i+1]), sum(payout_h[max(0, i-29):i+1])/len(payout_h[max(0, i-29):i+1]), sum(store_payout_h[max(0, i-29):i+1])/len(store_payout_h[max(0, i-29):i+1])
        row = [d_str, dow_names[datetime.strptime(d_str, "%Y/%m/%d").weekday()], "", t_d, int(t_d/u_cnt), int(t_g/u_cnt), f"{(t_g*3+t_d)/(t_g*3)*100:.1f}%", f"{(len([u for u in day_data.values() if u['games']>=5000 and u['diff']>0])/u_cnt*100):.1f}%"]
        for u in valid_units: row.append(day_data[u]['diff'] if u in day_data else "")
        row += ["", ma7, ma30, s_ma30]
        data_rows.append(row)
    ws.update(values=[data_header] + data_rows, range_name='A80')

    # --- STEP 6: グラフ & 装飾 ---
    meta = doc.fetch_sheet_metadata(); charts = next(s for s in meta['sheets'] if s['properties']['sheetId'] == s_id).get('charts', [])
    reqs = [{"deleteEmbeddedObject": {"objectId": c['chartId']}} for c in charts]
    l_row, l_col = len(data_rows) + 80, len(data_header)
    reqs.append({"addChart": {"chart": {"spec": {"title": "トレンド比較 (青:MA7 赤:MA30 灰:店全体30MA)", "basicChart": {"chartType": "LINE", "legendPosition": "BOTTOM_LEGEND", "axis": [{"position": "BOTTOM_AXIS"}, {"position": "LEFT_AXIS", "viewWindowOptions": {"viewWindowMin": 95, "viewWindowMax": 110, "viewWindowMode": "EXPLICIT"}}],
        "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 79, "endRowIndex": l_row, "startColumnIndex": 0, "endColumnIndex": 1}]}}}],
        "series": [{"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 79, "endRowIndex": l_row, "startColumnIndex": l_col+1, "endColumnIndex": l_col+2}]}}, "color": {"blue": 1.0}, "lineStyle": {"width": 2}},
                   {"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 79, "endRowIndex": l_row, "startColumnIndex": l_col+2, "endColumnIndex": l_col+3}]}}, "color": {"red": 1.0}, "lineStyle": {"width": 3}},
                   {"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 79, "endRowIndex": l_row, "startColumnIndex": l_col+3, "endColumnIndex": l_col+4}]}}, "color": {"red": 0.8, "green": 0.8, "blue": 0.8}, "lineStyle": {"width": 2}}]}},
        "position": {"overlayPosition": {"anchorCell": {"sheetId": s_id, "rowIndex": 34, "columnIndex": 0}, "widthPixels": 3200, "heightPixels": 450}}}}})

    # PMの列幅設定を尊重するため、以下の列幅強制リセットはコメントアウト
    # reqs.append({"updateDimensionProperties": {"range": {"sheetId": s_id, "dimension": "COLUMNS", "startIndex": 3, "endIndex": l_col}, "properties": {"pixelSize": 60}, "fields": "pixelSize"}})

    for i, d_str in enumerate(target_dates):
        dt = datetime.strptime(d_str, "%Y/%m/%d")
        color = {"red": 1, "green": 0, "blue": 0} if dt.weekday()==6 or jpholiday.is_holiday(dt) else ({"red": 0, "green": 0, "blue": 1} if dt.weekday()==5 else {"red": 0, "green": 0, "blue": 0})
        reqs.append({"updateCells": {"range": {"sheetId": s_id, "startRowIndex": 79+i, "endRowIndex": 80+i, "startColumnIndex": 0, "endColumnIndex": 2}, "rows": [{"values": [{"userEnteredFormat": {"textFormat": {"foregroundColor": color}}}, {"userEnteredFormat": {"textFormat": {"foregroundColor": color}}}]}], "fields": "userEnteredFormat.textFormat.foregroundColor"}})
    
    doc.batch_update({"requests": reqs})
    print("\n   -> Version 5.91 完成 (書式保護モード)")

async def main():
    print(f"\n--- Ver.5.91 起動 (Format Protected) ---")
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