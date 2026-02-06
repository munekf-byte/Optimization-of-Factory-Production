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
# BLOCK: 2. 統計エンジン (信憑性担保)
# ==========================================
def get_thresholds(values):
    """上位15%, 30%, 下位20% の境界値を算出"""
    nums = sorted([float(str(v).replace('%', '')) for v in values if v != "" and v is not None])
    if not nums: return None
    n = len(nums)
    return {
        'top15': nums[max(0, math.ceil(n * 0.85) - 1)],
        'top30': nums[max(0, math.ceil(n * 0.70) - 1)],
        'btm20': nums[max(0, math.ceil(n * 0.20) - 1)]
    }

def get_surgical_format(val, thr):
    """相対評価に基づく文字色・太字の決定"""
    try: v = float(str(val).replace('%', ''))
    except: return {"red": 0, "green": 0, "blue": 0}, False
    color, bold = {"red": 0, "green": 0, "blue": 0}, False
    if thr:
        if v >= thr['top15']: color, bold = {"red": 0, "green": 0, "blue": 0.9}, True
        elif v >= thr['top30']: color, bold = {"red": 0, "green": 0, "blue": 0.9}, False
        elif v <= thr['btm20']: color, bold = {"red": 0.9, "green": 0, "blue": 0}, True
        elif v < 0: color, bold = {"red": 0.9, "green": 0, "blue": 0}, False
    return color, bold

def split_periods_3(model_data, sorted_dates):
    if not sorted_dates: return []
    first_date = sorted_dates[0]
    prev_units = set(model_data[first_date].keys()) if first_date in model_data else set()
    break_points = []
    for d in sorted_dates:
        curr_units = set(model_data[d].keys()) if d in model_data else set()
        if curr_units and curr_units != prev_units:
            break_points.append(d); prev_units = curr_units
    if not break_points and len(sorted_dates) >= 3:
        n = len(sorted_dates)
        break_points = [sorted_dates[n//3], sorted_dates[2*n//3]]
    periods, start_idx = [], 0
    for bp in break_points + [None]:
        if bp:
            end_idx = sorted_dates.index(bp)
            periods.append(sorted_dates[start_idx:end_idx]); start_idx = end_idx
        else: periods.append(sorted_dates[start_idx:])
    return [p for p in periods if p][:3]

# ==========================================
# BLOCK: 3. メイン分析エンジン
# ==========================================
async def execute_single_analysis(doc, conf):
    print(f"   > 分析開始: {conf['target_model']}")
    
    # --- 1. 期間・店舗情報の事前取得 ---
    tmp_ws = doc.worksheet(TEMPLATE_SHEET)
    # B4:C4から解析期間を直接読み取る
    p_vals = tmp_ws.batch_get(['B4', 'C4'])
    start_limit = datetime.strptime(p_vals[0][0][0], "%Y/%m/%d")
    end_limit = datetime.strptime(p_vals[1][0][0], "%Y/%m/%d")
    print(f"   > 期間拘束: {start_limit.date()} 〜 {end_limit.date()}")

    # --- 2. データ抽出 (期間フィルタ厳守) ---
    raw_data, unit_appearance = [], collections.defaultdict(list)
    store_daily_stats = collections.defaultdict(lambda: {'diff': 0, 'games': 0})
    
    with open(LOCAL_DATABASE, mode='r', encoding='utf-8-sig') as f:
        reader = csv.reader(f); next(reader, None)
        for row in reader:
            if len(row) < 6: continue
            d_date, d_store, d_model, d_unit, d_diff, d_games = [c.strip() for c in row]
            dt = datetime.strptime(d_date, "%Y/%m/%d")
            
            # 店舗フィルタ
            if conf['store'] not in d_store: continue
            store_daily_stats[d_date]['diff'] += int(d_diff)
            store_daily_stats[d_date]['games'] += int(d_games)
            
            # 機種 ＆ 期間フィルタ (ここが信憑性の肝)
            if conf['target_model'] == d_model and start_limit <= dt <= end_limit:
                unit_appearance[int(d_unit)].append(dt)
                raw_data.append({'date': d_date, 'unit': int(d_unit), 'diff': int(d_diff), 'games': int(d_games)})

    valid_units = sorted(list(unit_appearance.keys()))
    if not valid_units:
        print("   ! 該当期間内にデータが見つかりませんでした。")
        return

    # --- 3. 集計ロジック ---
    model_data, all_diffs, all_games = collections.defaultdict(dict), [], []
    payout_h, store_payout_h = [], []
    target_dates = sorted(list(set(r['date'] for r in raw_data)))
    dow_stats, digit_stats = collections.defaultdict(list), collections.defaultdict(list)

    for i, d_str in enumerate(target_dates):
        day_units = [r for r in raw_data if r['date'] == d_str]
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

    # --- 4. シート建築 (更地化 ＆ 複製) ---
    try:
        old = doc.worksheet(SINGLE_SHEET)
        doc.del_worksheet(old)
    except WorksheetNotFound: pass
    ws = doc.duplicate_sheet(tmp_ws.id, insert_sheet_index=1, new_sheet_name=SINGLE_SHEET)
    s_id = ws.id
    # 右側のゴーストデータを完全に消去するため、データエリアをクリア
    ws.batch_clear(['A8:O33', 'A81:ZZ2000'])

    # --- 5. 精密マッピング (注入) ---
    def safe_avg(lst): return int(sum(lst)/len(lst)) if lst else 0
    avg_d, avg_g = safe_avg(all_diffs), safe_avg(all_games)
    avg_r = (sum(all_games)*3+sum(all_diffs))/(sum(all_games)*3)*100 if sum(all_games)>0 else 100.0
    
    ws.update(values=[[conf['store']], [conf['target_model']]], range_name='B1:B2')
    ws.update_acell('G3', f"{avg_d}枚")
    ws.update_acell('J3', f"{avg_g}G")
    ws.update_acell('M3', f"{avg_r:.1f}%")

    # 曜日 & 末尾
    ws.update(values=[[f"{safe_avg(dow_stats[i])}枚"] for i in range(7)], range_name='B8:B14')
    ws.update(values=[[f"{safe_avg(digit_stats[i])}枚"] for i in range(10)], range_name='E8:E17')

    # BEST/WORST 計算 & 注入
    def get_rank(dates):
        u_s = collections.defaultdict(list)
        for d in dates:
            for u, v in model_data[d].items(): u_s[u].append(v['diff'])
        res = sorted([(u, int(sum(v)/len(v))) for u, v in u_s.items()], key=lambda x: x[1], reverse=True)
        return res[:5], res[-5:][::-1]

    periods = split_periods_3(model_data, target_dates)
    p_ranks = [get_rank(target_dates)] + [get_rank(p) for p in periods]
    for idx, (b, w) in enumerate(p_ranks):
        if idx > 3: break
        col = ['B','E','H','K'][idx]
        ws.update(values=[[f"{r[0]}番台", f"{r[1]}枚"] for r in b], range_name=f'{col}22')
        ws.update(values=[[f"{r[0]}番台", f"{r[1]}枚"] for r in w], range_name=f'{col}28')

    # 期間別サマリー (H9:M11)
    for i, p in enumerate(periods):
        p_d = [model_data[d] for d in p if d in model_data]
        p_ds = sum(sum(u['diff'] for u in day.values()) for day in p_d)
        p_gs = sum(sum(u['games'] for u in day.values()) for day in p_d)
        p_uc = sum(len(day) for day in p_d)
        p_avg_r = (p_gs*3+p_ds)/(p_gs*3)*100 if p_gs > 0 else 100.0
        row = [[f"{p[0]}〜{p[-1]}", "", "", f"{int(p_ds/p_uc)}枚", f"{p_avg_r:.1f}%", f"{int(p_gs/p_uc)}G"]]
        ws.update(values=row, range_name=f'H{9+i}')

    # --- 6. データ倉庫 (A81〜) ---
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

    # --- 7. 三次元書式拡張 ＆ 外科手術 ---
    l_row, l_col = len(data_rows) + 81, len(data_header)
    reqs = []
    # 書式の自動展開 (I81横, D82:H82縦, I82面)
    reqs.append({"copyPaste": {"source": {"sheetId": s_id, "startRowIndex": 80, "endRowIndex": 81, "startColumnIndex": 8, "endColumnIndex": 9},
                               "destination": {"sheetId": s_id, "startRowIndex": 80, "endRowIndex": 81, "startColumnIndex": 9, "endColumnIndex": l_col}, "pasteType": "PASTE_FORMAT"}})
    reqs.append({"copyPaste": {"source": {"sheetId": s_id, "startRowIndex": 81, "endRowIndex": 82, "startColumnIndex": 3, "endColumnIndex": 8},
                               "destination": {"sheetId": s_id, "startRowIndex": 82, "endRowIndex": l_row, "startColumnIndex": 3, "endColumnIndex": 8}, "pasteType": "PASTE_FORMAT"}})
    reqs.append({"copyPaste": {"source": {"sheetId": s_id, "startRowIndex": 81, "endRowIndex": 82, "startColumnIndex": 8, "endColumnIndex": 9},
                               "destination": {"sheetId": s_id, "startRowIndex": 81, "endRowIndex": l_row, "startColumnIndex": 8, "endColumnIndex": l_col}, "pasteType": "PASTE_FORMAT"}})

    # 相対評価の書き込み
    col_thresholds = {c: get_thresholds([r[c] for r in data_rows if r[c] != ""]) for c in range(3, l_col)}
    for i, row in enumerate(data_rows):
        row_idx, row_formats = 81 + i, []
        for c in range(3, l_col):
            val = row[c]
            color, bold = get_surgical_format(val, col_thresholds.get(c))
            fmt = {"textFormat": {"foregroundColor": color, "bold": bold}}
            if c >= 8 and val == "": fmt["backgroundColor"] = {"red": 0.9, "green": 0.9, "blue": 0.9} # 空白
            row_formats.append({"userEnteredFormat": fmt})
        reqs.append({"updateCells": {"range": {"sheetId": s_id, "startRowIndex": row_idx, "endRowIndex": row_idx + 1, "startColumnIndex": 3, "endColumnIndex": l_col},
                                     "rows": [{"values": row_formats}], "fields": "userEnteredFormat.textFormat,userEnteredFormat.backgroundColor"}})

    doc.batch_update({"requests": reqs})
    print(f"   -> {conf['target_model']} 解析完了。期間フィルタ厳守モデル。")

# ==========================================
# BLOCK: 5. 同期 ＆ メインループ
# ==========================================
async def sync_store_list(doc):
    try:
        unique_stores = set()
        with open(LOCAL_DATABASE, mode='r', encoding='utf-8-sig') as f:
            reader = csv.reader(f); next(reader, None) 
            for row in reader:
                if len(row) > 1: unique_stores.add(row[1])
        idx_ws = doc.worksheet(INDEX_SHEET)
        idx_ws.clear()
        idx_ws.update(values=[["店舗リスト(AutoSync)"]] + [[s] for s in sorted(list(unique_stores))], range_name='A1')
        print(f"   -> 店舗同期完了: {len(unique_stores)}店舗。")
    except Exception as e: print(f"   ! 同期エラー: {e}")

async def main():
    print(f"\n--- Ver.5.98 起動 (Clean Integrity) ---")
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
        except Exception as e: print(f"\nError: {e}"); await asyncio.sleep(5)
        await asyncio.sleep(15)

if __name__ == "__main__": asyncio.run(main())