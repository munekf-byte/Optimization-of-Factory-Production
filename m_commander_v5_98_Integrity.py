# --- VERSION: m_commander_v5_98_Integrity ---
# 1. 期間フィルタリング(B4:C4)の厳格化
# 2. ゴーストデータ完全抹消 (A1:ZZ2000強制クリア)
# 3. マッピング座標の完全再定義

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
# BLOCK: 2. 統計エンジン (強化版)
# ==========================================
def get_thresholds(values):
    nums = sorted([float(str(v).replace('%', '')) for v in values if v != "" and v is not None])
    if not nums: return None
    n = len(nums)
    return {
        'top15': nums[max(0, math.ceil(n * 0.85) - 1)],
        'top30': nums[max(0, math.ceil(n * 0.70) - 1)],
        'btm20': nums[max(0, math.ceil(n * 0.20) - 1)]
    }

def get_surgical_format(val, thr):
    try: v = float(str(val).replace('%', ''))
    except: return {"red": 0, "green": 0, "blue": 0}, False
    color, bold = {"red": 0, "green": 0, "blue": 0}, False
    if thr:
        if v >= thr['top15']: color, bold = {"red": 0, "green": 0, "blue": 0.9}, True
        elif v >= thr['top30']: color, bold = {"red": 0, "green": 0, "blue": 0.9}, False
        elif v <= thr['btm20']: color, bold = {"red": 0.9, "green": 0, "blue": 0}, True
        elif v < 0: color, bold = {"red": 0.9, "green": 0, "blue": 0}, False
    return color, bold

# ==========================================
# BLOCK: 3. 解析エンジン (信憑性重視)
# ==========================================
async def execute_single_analysis(doc, conf):
    # --- 1. 期間の読み取り (信憑性の肝) ---
    tmp_ws = doc.worksheet(TEMPLATE_SHEET)
    period_vals = tmp_ws.batch_get(['B4', 'C4'])
    start_limit = datetime.strptime(period_vals[0][0][0], "%Y/%m/%d")
    end_limit = datetime.strptime(period_vals[1][0][0], "%Y/%m/%d")
    print(f"   > 期間フィルタ適用: {start_limit.date()} 〜 {end_limit.date()}")

    # --- 2. データ抽出 (厳格なフィルタ) ---
    raw_data, unit_appearance = [], collections.defaultdict(list)
    store_daily_stats = collections.defaultdict(lambda: {'diff': 0, 'games': 0})
    
    with open(LOCAL_DATABASE, mode='r', encoding='utf-8-sig') as f:
        reader = csv.reader(f); next(reader, None)
        for row in reader:
            if len(row) < 6: continue
            d_date, d_store, d_model, d_unit, d_diff, d_games = [c.strip() for c in row]
            dt = datetime.strptime(d_date, "%Y/%m/%d")
            
            # 店舗・日付フィルタ
            if conf['store'] not in d_store: continue
            store_daily_stats[d_date]['diff'] += int(d_diff)
            store_daily_stats[d_date]['games'] += int(d_games)
            
            # 機種・期間内フィルタ
            if conf['target_model'] == d_model and start_limit <= dt <= end_limit:
                unit_appearance[int(d_unit)].append(dt)
                raw_data.append({'date': d_date, 'unit': int(d_unit), 'diff': int(d_diff), 'games': int(d_games)})

    valid_units = sorted([u for u in unit_appearance.keys()]) # 期間内に存在すればOKに変更(信憑性優先)
    if not valid_units: return

    # --- 3. 集計 ---
    model_data, payout_h, store_payout_h = collections.defaultdict(dict), [], []
    all_diffs, all_games = [], []
    target_dates = sorted(list(set(r['date'] for r in raw_data)))
    dow_stats, digit_stats = collections.defaultdict(list), collections.defaultdict(list)

    for i, d_str in enumerate(target_dates):
        day_units = [r for r in raw_data if r['date'] == d_str]
        if not day_units: continue
        t_d, t_g = sum(r['diff'] for r in day_units), sum(r['games'] for r in day_units)
        payout_h.append(((t_g*3+t_d)/(t_g*3)*100) if t_g > 0 else 100)
        for r in day_units:
            model_data[d_str][r['unit']] = {'diff': r['diff'], 'games': r['games']}
            all_diffs.append(r['diff']); all_games.append(r['games'])
        s_d, s_g = store_daily_stats[d_str]['diff'], store_daily_stats[d_str]['games']
        store_payout_h.append(((s_g*3+s_d)/(s_g*3)*100) if s_g > 0 else 100)
        dt = datetime.strptime(d_str, "%Y/%m/%d")
        dow_stats[dt.weekday()].append(t_d/len(day_units))
        digit_stats[dt.day % 10].append(t_d/len(day_units))

    # --- 4. シート構築 (ゴースト対策) ---
    try: old = doc.worksheet(SINGLE_SHEET); doc.del_worksheet(old)
    except WorksheetNotFound: pass
    ws = doc.duplicate_sheet(tmp.id, insert_sheet_index=1, new_sheet_name=SINGLE_SHEET)
    s_id = ws.id
    ws.batch_clear(['A1:ZZ2000']) # 強制クリア
    # テンプレートの「画」を復元（値なしコピー）
    ws.update(values=tmp.get_all_values(), range_name='A1')

    # --- 5. 精密マッピング (注入) ---
    def safe_avg(lst): return int(sum(lst)/len(lst)) if lst else 0
    
    avg_d, avg_g = safe_avg(all_diffs), safe_avg(all_games)
    avg_r = (sum(all_games)*3+sum(all_diffs))/(sum(all_games)*3)*100 if sum(all_games)>0 else 100.0
    
    ws.update(values=[[conf['store']], [conf['target_model']]], range_name='B1:B2')
    ws.update(values=[[f"{avg_d}枚"], [f"{avg_g}G"], [f"{avg_r:.1f}%"]], range_name='G3:M3:step=3') # G3, J3, M3へ
    ws.update_acell('G3', f"{avg_d}枚")
    ws.update_acell('J3', f"{avg_g}G")
    ws.update_acell('M3', f"{avg_r:.1f}%")

    # BEST/WORST (座標のズレを修正)
    def get_period_stats(dates):
        u_stats = collections.defaultdict(list)
        for d in dates:
            for u, v in model_data[d].items(): u_stats[u].append(v['diff'])
        res = [(u, int(sum(v)/len(v))) for u, v in u_stats.items()]
        return sorted(res, key=lambda x: x[1], reverse=True)[:5], sorted(res, key=lambda x: x[1])[:5]

    periods = split_periods_3(model_data, target_dates)
    total_b, total_w = get_period_stats(target_dates)
    p_stats_list = [get_period_stats(p) for p in periods]

    for idx, (b, w) in enumerate([(total_b, total_w)] + p_stats_list):
        if idx > 3: break
        col = ['B','E','H','K'][idx]
        ws.update(values=[[f"{r[0]}番台", f"{r[1]}枚"] for r in b], range_name=f'{col}22')
        ws.update(values=[[f"{r[0]}番台", f"{r[1]}枚"] for r in w], range_name=f'{col}28')

    # --- 6. データ倉庫 ---
    data_header = ["日付", "曜日", "イベントログ", "総計", "台平均", "平均G", "機械割", "粘り勝率"] + [f"{u}番" for u in valid_units]
    data_rows = []
    for i, d_str in enumerate(target_dates):
        d_units = model_data[d_str]; u_cnt = len(d_units)
        t_d, t_g = sum(u['diff'] for u in d_units.values()), sum(u['games'] for u in d_units.values())
        day_avg_r = (t_g*3+t_d)/(t_g*3)*100 if t_g > 0 else 100.0
        row = [d_str, ["月","火","水","木","金","土","日"][datetime.strptime(d_str, "%Y/%m/%d").weekday()], "", t_d, int(t_d/u_cnt), int(t_g/u_cnt), f"{day_avg_r:.1f}%", ""]
        for u in valid_units: row.append(d_units[u]['diff'] if u in d_units else "")
        data_rows.append(row)
    ws.update(values=[data_header] + data_rows, range_name='A81')

    # --- 7. 外科手術 (書式拡張) ---
    l_row, l_col = len(data_rows) + 81, len(data_header)
    reqs = []
    # I81の書式を右端まで
    reqs.append({"copyPaste": {"source": {"sheetId": s_id, "startRowIndex": 80, "endRowIndex": 81, "startColumnIndex": 8, "endColumnIndex": 9},
                               "destination": {"sheetId": s_id, "startRowIndex": 80, "endRowIndex": 81, "startColumnIndex": 9, "endColumnIndex": l_col}, "pasteType": "PASTE_FORMAT"}})
    # I82の書式を全面へ
    reqs.append({"copyPaste": {"source": {"sheetId": s_id, "startRowIndex": 81, "endRowIndex": 82, "startColumnIndex": 8, "endColumnIndex": 9},
                               "destination": {"sheetId": s_id, "startRowIndex": 81, "endRowIndex": l_row, "startColumnIndex": 8, "endColumnIndex": l_col}, "pasteType": "PASTE_FORMAT"}})

    col_thresholds = {c: get_thresholds([r[c] for r in data_rows if r[c] != ""]) for c in range(3, l_col)}
    for i, row in enumerate(data_rows):
        row_idx, row_formats = 81 + i, []
        for c in range(3, l_col):
            val = row[c]
            color, bold = get_surgical_format(val, col_thresholds.get(c))
            fmt = {"textFormat": {"foregroundColor": color, "bold": bold}}
            if c >= 8 and val == "": fmt["backgroundColor"] = {"red": 0.9, "green": 0.9, "blue": 0.9}
            row_formats.append({"userEnteredFormat": fmt})
        reqs.append({"updateCells": {"range": {"sheetId": s_id, "startRowIndex": row_idx, "endRowIndex": row_idx + 1, "startColumnIndex": 3, "endColumnIndex": l_col},
                                     "rows": [{"values": row_formats}], "fields": "userEnteredFormat.textFormat,userEnteredFormat.backgroundColor"}})

    doc.batch_update({"requests": reqs})
    print(f"   -> {conf['target_model']} 解析完了。信憑性フィルタ適用済。")

# ... (mainループ等は継承) ...