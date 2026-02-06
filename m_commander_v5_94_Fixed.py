# --- VERSION: m_commander_v5_94_Fixed ---
# 修正内容: 消失していた sync_store_list を完全復旧し、外科手術ロジックと統合

import gspread
from gspread.exceptions import WorksheetNotFound
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import asyncio
import csv
import collections
import jpholiday
import math

# ==========================================
# BLOCK: 1. 固定設定
# ==========================================
SPREADSHEET_KEY = "1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I"
CONFIG_SHEET    = "分析設定"
SINGLE_SHEET    = "機種別分析"
TEMPLATE_SHEET  = "TEMPLATE_SINGLE"
INDEX_SHEET     = "機種目録" 
LOCAL_DATABASE  = "/Users/macuser/Desktop/minrepo_project/minrepo_database.csv"

# ==========================================
# BLOCK: 2. 同期・統計エンジン
# ==========================================
async def sync_store_list(doc):
    """店舗リストの同期"""
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

def get_thresholds(values):
    """上位15%, 30%, 下位20% の境界値を算出"""
    nums = sorted([float(str(v).replace('%', '')) for v in values if v != ""])
    if not nums: return None
    n = len(nums)
    return {
        'top15': nums[max(0, math.ceil(n * 0.85) - 1)],
        'top30': nums[max(0, math.ceil(n * 0.70) - 1)],
        'btm20': nums[max(0, math.ceil(n * 0.20) - 1)]
    }

def get_surgical_format(val, thr):
    """相対評価に基づく文字色・太字の決定"""
    try:
        v = float(str(val).replace('%', ''))
    except:
        return {"red": 0, "green": 0, "blue": 0}, False

    color = {"red": 0, "green": 0, "blue": 0} # 黒
    bold = False

    if thr:
        if v >= thr['top15']:
            color, bold = {"red": 0, "green": 0, "blue": 0.9}, True # 青太字
        elif v >= thr['top30']:
            color, bold = {"red": 0, "green": 0, "blue": 0.9}, False # 青
        elif v <= thr['btm20']:
            color, bold = {"red": 0.9, "green": 0, "blue": 0}, True # 赤太字
        elif v < 0:
            color, bold = {"red": 0.9, "green": 0, "blue": 0}, False # 赤文字

    return color, bold

# ==========================================
# BLOCK: 3. 解析ロジック継承
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

    model_data, payout_h, store_payout_h = collections.defaultdict(dict), [], []
    all_diffs, all_games = [], []
    target_dates = sorted(list(set(r['date'] for r in raw_data)))
    
    for d_str in target_dates:
        day_units = [r for r in raw_data if r['date'] == d_str and r['unit'] in valid_units]
        t_d, t_g = sum(r['diff'] for r in day_units), sum(r['games'] for r in day_units)
        payout_h.append(((t_g*3+t_d)/(t_g*3)*100) if t_g > 0 else 100)
        for r in day_units:
            model_data[d_str][r['unit']] = {'diff': r['diff'], 'games': r['games']}
            all_diffs.append(r['diff']); all_games.append(r['games'])
        s_d, s_g = store_daily_stats[d_str]['diff'], store_daily_stats[d_str]['games']
        store_payout_h.append(((s_g*3+s_d)/(s_g*3)*100) if s_g > 0 else 100)

    periods = split_periods_3(model_data, target_dates)
    total_best, total_worst = get_period_rankings_5(model_data, target_dates)
    p_res = [{'dates': p, 'best': get_period_rankings_5(model_data, p)[0], 'worst': get_period_rankings_5(model_data, p)[1]} for p in periods]

    try:
        old_ws = doc.worksheet(SINGLE_SHEET)
        doc.del_worksheet(old_ws)
    except WorksheetNotFound: pass
    
    try:
        tmp_ws = doc.worksheet(TEMPLATE_SHEET)
        ws = doc.duplicate_sheet(tmp_ws.id, insert_sheet_index=1, new_sheet_name=SINGLE_SHEET)
    except WorksheetNotFound:
        print(f"   ! エラー: {TEMPLATE_SHEET} シートが見つかりません。")
        return
    s_id = ws.id

    avg_d_total, avg_g_total = int(sum(all_diffs)/len(all_diffs)) if all_diffs else 0, int(sum(all_games)/len(all_games)) if all_games else 0
    avg_r_total = ((sum(all_games)*3+sum(all_diffs))/(sum(all_games)*3)*100) if sum(all_games)>0 else 0
    dash = [[""] * 15 for _ in range(33)]
    dash[0][1], dash[1][1] = conf['store'], conf['target_model']
    dash[2][6], dash[2][9], dash[2][12] = f"{avg_d_total}枚", f"{avg_g_total}G", f"{avg_r_total:.1f}%"
    dash[3][1], dash[3][3] = target_dates[0], target_dates[-1]
    for i, p in enumerate(p_res):
        p_d = [model_data[d] for d in p['dates'] if d in model_data]
        if not p_d: continue
        p_ds, p_gs, p_uc = sum(sum(u['diff'] for u in day.values()) for day in p_d), sum(sum(u['games'] for u in day.values()) for day in p_d), sum(len(day) for day in p_d)
        dash[8+i][7], dash[8+i][10], dash[8+i][11], dash[8+i][12] = f"{p['dates'][0]}〜{p['dates'][-1]}", f"{int(p_ds/p_uc)}枚", f"{(p_gs*3+p_ds)/(p_gs*3)*100:.1f}%", f"{int(p_gs/p_uc)}G"
    for col_idx, r in enumerate([(total_best, total_worst)] + [(p['best'], p['worst']) for p in p_res]):
        if col_idx > 3: break
        for j in range(5):
            if j < len(r[0]): dash[21+j][col_idx*3], dash[21+j][col_idx*3+1] = f"{r[0][j][0]}番台", f"{r[0][j][1]}枚"
            if j < len(r[1]): dash[27+j][col_idx*3], dash[27+j][col_idx*3+1] = f"{r[1][j][0]}番台", f"{r[1][j][1]}枚"
    ws.update(values=dash, range_name='A1')

    data_header = ["日付", "曜日", "イベントログ", "総計", "台平均", "平均G", "機械割", "粘り勝率"] + [f"{u}番" for u in valid_units]
    data_rows = []
    for i, d_str in enumerate(target_dates):
        day_data = model_data[d_str]; u_cnt = len(day_data)
        if u_cnt == 0: continue
        t_d, t_g = sum(u['diff'] for u in day_data.values()), sum(u['games'] for u in day_data.values())
        ma7, ma30, s_ma30 = sum(payout_h[max(0, i-6):i+1])/len(payout_h[max(0, i-6):i+1]), sum(payout_h[max(0, i-29):i+1])/len(payout_h[max(0, i-29):i+1]), sum(store_payout_h[max(0, i-29):i+1])/len(store_payout_h[max(0, i-29):i+1])
        row = [d_str, dow_names[datetime.strptime(d_str, "%Y/%m/%d").weekday()], "", t_d, int(t_d/u_cnt), int(t_g/u_cnt), f"{(t_g*3+t_d)/(t_g*3)*100:.1f}%", f"{(len([u for u in day_data.values() if u['games']>=5000 and u['diff']>0])/u_cnt*100):.1f}%"]
        for u in valid_units: row.append(day_data[u]['diff'] if u in day_data else "")
        row += ["", ma7, ma30, s_ma30]; data_rows.append(row)
    ws.update(values=[data_header] + data_rows, range_name='A81')

    l_row, l_col = len(data_rows) + 81, len(data_header)
    reqs = []
    if len(valid_units) > 1:
        reqs.append({"copyPaste": {"source": {"sheetId": s_id, "startRowIndex": 80, "endRowIndex": 81, "startColumnIndex": 8, "endColumnIndex": 9},
                                   "destination": {"sheetId": s_id, "startRowIndex": 80, "endRowIndex": 81, "startColumnIndex": 9, "endColumnIndex": l_col}, "pasteType": "PASTE_FORMAT"}})
    reqs.append({"copyPaste": {"source": {"sheetId": s_id, "startRowIndex": 81, "endRowIndex": 82, "startColumnIndex": 3, "endColumnIndex": l_col},
                               "destination": {"sheetId": s_id, "startRowIndex": 82, "endRowIndex": l_row, "startColumnIndex": 3, "endColumnIndex": l_col}, "pasteType": "PASTE_FORMAT"}})

    col_thresholds = {}
    for c in range(3, l_col):
        col_vals = [r[c] for r in data_rows if r[c] != ""]
        col_thresholds[c] = get_thresholds(col_vals)

    for i, row in enumerate(data_rows):
        row_idx = 81 + i
        row_formats = []
        for c in range(3, l_col):
            color, bold = get_surgical_format(row[c], col_thresholds.get(c))
            row_formats.append({"userEnteredFormat": {"textFormat": {"foregroundColor": color, "bold": bold}}})
        reqs.append({"updateCells": {"range": {"sheetId": s_id, "startRowIndex": row_idx, "endRowIndex": row_idx + 1, "startColumnIndex": 3, "endColumnIndex": l_col},
                                     "rows": [{"values": row_formats}], "fields": "userEnteredFormat.textFormat"}})

    reqs.append({"addChart": {"chart": {"spec": {"title": "トレンド比較", "basicChart": {"chartType": "LINE", "legendPosition": "BOTTOM_LEGEND", "axis": [{"position": "BOTTOM_AXIS"}, {"position": "LEFT_AXIS", "viewWindowOptions": {"viewWindowMin": 95, "viewWindowMax": 110, "viewWindowMode": "EXPLICIT"}}],
        "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 80, "endRowIndex": l_row, "startColumnIndex": 0, "endColumnIndex": 1}]}}}],
        "series": [{"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 80, "endRowIndex": l_row, "startColumnIndex": l_col+1, "endColumnIndex": l_col+2}]}}, "color": {"blue": 1.0}},
                   {"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 80, "endRowIndex": l_row, "startColumnIndex": l_col+2, "endColumnIndex": l_col+3}]}}, "color": {"red": 1.0}},
                   {"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 80, "endRowIndex": l_row, "startColumnIndex": l_col+3, "endColumnIndex": l_col+4}]}}, "color": {"red": 0.8, "green": 0.8, "blue": 0.8}}]}},
        "position": {"overlayPosition": {"anchorCell": {"sheetId": s_id, "rowIndex": 34, "columnIndex": 0}, "widthPixels": 3200, "heightPixels": 450}}}}})
    doc.batch_update({"requests": reqs})
    print(f"\n   -> Version 5.94 完成 (Surgical Edition)")

async def main():
    print(f"\n--- Ver.5.94 起動 (The Surgical Edition) ---")
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