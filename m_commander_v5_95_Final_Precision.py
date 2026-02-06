# --- VERSION: m_commander_v5_95_Final_Precision ---
# 1. テンプレートのラベル（タイトル等）を完全維持
# 2. 書式の三次元拡張（I81横, D82:H82縦, I82面）を実装
# 3. 空白セルを薄グレーで塗りつぶすロジックを追加

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
# BLOCK: 2. ユーティリティ (統計計算 & クリーニング)
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
    except Exception as e: print(f"   ! 同期エラー: {e}")

def get_thresholds(values):
    """上位15%, 30%, 下位20% の境界値を算出（%除去対応）"""
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
    try:
        v = float(str(val).replace('%', ''))
    except:
        return {"red": 0, "green": 0, "blue": 0}, False
    color, bold = {"red": 0, "green": 0, "blue": 0}, False
    if thr:
        if v >= thr['top15']: color, bold = {"red": 0, "green": 0, "blue": 0.9}, True
        elif v >= thr['top30']: color, bold = {"red": 0, "green": 0, "blue": 0.9}, False
        elif v <= thr['btm20']: color, bold = {"red": 0.9, "green": 0, "blue": 0}, True
        elif v < 0: color, bold = {"red": 0.9, "green": 0, "blue": 0}, False
    return color, bold

# = [解析ロジックは継承] =
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
    return sorted(u_avgs, key=lambda x: x[1], reverse=True)[:5], sorted(u_avgs, key=lambda x: x[1])[:5]

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
        break_points = [sorted_dates[len(sorted_dates)//3], sorted_dates[2*len(sorted_dates)//3]]
    periods, start_idx = [], 0
    for bp in break_points + [None]:
        if bp:
            end_idx = sorted_dates.index(bp)
            periods.append(sorted_dates[start_idx:end_idx]); start_idx = end_idx
        else: periods.append(sorted_dates[start_idx:])
    return [p for p in periods if p][:3]

# ==========================================
# BLOCK: 4. メイン分析エンジン
# ==========================================
async def execute_single_analysis(doc, conf):
    print(f"   > 解析開始: {conf['target_model']}")
    dow_names = ["月", "火", "水", "木", "金", "土", "日"]
    raw_data, store_daily_stats = [], collections.defaultdict(lambda: {'diff': 0, 'games': 0})
    unit_appearance = collections.defaultdict(list)

    with open(LOCAL_DATABASE, mode='r', encoding='utf-8-sig') as f:
        reader = csv.reader(f); next(reader, None)
        for row in reader:
            if len(row) < 6: continue
            d_date, d_store, d_model, d_unit, d_diff, d_games = [c.strip() for c in row]
            if conf['store'] not in d_store: continue
            store_daily_stats[d_date]['diff'] += int(d_diff)
            store_daily_stats[d_date]['games'] += int(d_games)
            if conf['target_model'] in d_model:
                unit_appearance[int(d_unit)].append(datetime.strptime(d_date, "%Y/%m/%d"))
                raw_data.append({'date': d_date, 'unit': int(d_unit), 'diff': int(d_diff), 'games': int(d_games)})

    valid_units = sorted([u for u, dates in unit_appearance.items() if any((sorted(dates)[i+2] - sorted(dates)[i]).days <= 4 for i in range(len(dates)-2))])
    if not valid_units: return

    model_data, all_diffs, all_games = collections.defaultdict(dict), [], []
    payout_h, store_payout_h = [], []
    target_dates = sorted(list(set(r['date'] for r in raw_data)))
    dow_stats, digit_stats = collections.defaultdict(list), collections.defaultdict(list)

    for i, d_str in enumerate(target_dates):
        day_units = [r for r in raw_data if r['date'] == d_str and r['unit'] in valid_units]
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

    periods = split_periods_3(model_data, target_dates)
    p_res = [{'dates': p, 'best': get_period_rankings_5(model_data, p)[0], 'worst': get_period_rankings_5(model_data, p)[1]} for p in periods]
    total_best, total_worst = get_period_rankings_5(model_data, target_dates)

    # --- シート建築 (テンプレート複製) ---
    try: old = doc.worksheet(SINGLE_SHEET); doc.del_worksheet(old)
    except WorksheetNotFound: pass
    tmp = doc.worksheet(TEMPLATE_SHEET)
    ws = doc.duplicate_sheet(tmp.id, insert_sheet_index=1, new_sheet_name=SINGLE_SHEET)
    s_id = ws.id

    # --- Dashboard 値注入 (タイトルを壊さないよう個別Update) ---
    avg_d = int(sum(all_diffs)/len(all_diffs)) if all_diffs else 0
    avg_g = int(sum(all_games)/len(all_games)) if all_games else 0
    avg_r = ((sum(all_games)*3+sum(all_diffs))/(sum(all_games)*3)*100) if sum(all_games)>0 else 0
    
    # 基本情報
    ws.update(values=[[conf['store']], [conf['target_model']]], range_name='B1:B2')
    ws.update(values=[[target_dates[0]], [target_dates[-1]]], range_name='B4:C4')
    ws.update(values=[[f"{avg_d}枚", "", f"{avg_g}G", "", f"{avg_r:.1f}%"]], range_name='G3:K3')

    # 曜日 & 末尾
    dow_v = [[f"{int(sum(dow_stats[i])/len(dow_stats[i]))}枚" if dow_stats[i] else "0枚"] for i in range(7)]
    digit_v = [[f"{int(sum(digit_stats[i])/len(digit_stats[i]))}枚" if digit_stats[i] else "0枚"] for i in range(10)]
    ws.update(values=dow_v, range_name='B8:B14')
    ws.update(values=digit_v, range_name='E8:E17')

    # 期間別
    for i, p in enumerate(p_res):
        p_d = [model_data[d] for d in p['dates'] if d in model_data]
        if not p_d: continue
        p_ds = sum(sum(u['diff'] for u in day.values()) for day in p_d)
        p_gs = sum(sum(u['games'] for u in day.values()) for day in p_d)
        p_uc = sum(len(day) for day in p_d)
        row = [[f"{p['dates'][0]}〜{p['dates'][-1]}", "", "", f"{int(p_ds/p_uc)}枚", f"{(p_gs*3+p_ds)/(p_gs*3)*100:.1f}%", f"{int(p_gs/p_uc)}G"]]
        ws.update(values=row, range_name=f'I{9+i}:N{9+i}')

    # BEST/WORST
    for col_idx, res in enumerate([(total_best, total_worst)] + [(p['best'], p['worst']) for p in p_res]):
        if col_idx > 3: break
        start_col = ['B','E','H','K'][col_idx]
        best_v = [[f"{r[0]}番台", f"{r[1]}枚"] for r in res[0]]
        worst_v = [[f"{r[0]}番台", f"{r[1]}枚"] for r in res[1]]
        ws.update(values=best_v, range_name=f'{start_col}22:{chr(ord(start_col)+1)}26')
        ws.update(values=worst_v, range_name=f'{start_col}28:{chr(ord(start_col)+1)}32')

    # --- データ倉庫建築 ---
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

    # --- 書式拡張 ＆ 外科手術 ---
    l_row, l_col = len(data_rows) + 81, len(data_header)
    reqs = []
    
    # [1] ヘッダ書式横コピー (I81 -> I81:END81)
    reqs.append({"copyPaste": {"source": {"sheetId": s_id, "startRowIndex": 80, "endRowIndex": 81, "startColumnIndex": 8, "endColumnIndex": 9},
                               "destination": {"sheetId": s_id, "startRowIndex": 80, "endRowIndex": 81, "startColumnIndex": 9, "endColumnIndex": l_col}, "pasteType": "PASTE_FORMAT"}})
    # [2] D:H縦コピー (D82:H82 -> D83:H_END)
    reqs.append({"copyPaste": {"source": {"sheetId": s_id, "startRowIndex": 81, "endRowIndex": 82, "startColumnIndex": 3, "endColumnIndex": 8},
                               "destination": {"sheetId": s_id, "startRowIndex": 82, "endRowIndex": l_row, "startColumnIndex": 3, "endColumnIndex": 8}, "pasteType": "PASTE_FORMAT"}})
    # [3] I:END面コピー (I82 -> I82:END_END)
    reqs.append({"copyPaste": {"source": {"sheetId": s_id, "startRowIndex": 81, "endRowIndex": 82, "startColumnIndex": 8, "endColumnIndex": 9},
                               "destination": {"sheetId": s_id, "startRowIndex": 81, "endRowIndex": l_row, "startColumnIndex": 8, "endColumnIndex": l_col}, "pasteType": "PASTE_FORMAT"}})

    # [4] 相対評価文字色 + 空白グレー塗りつぶし
    col_thresholds = {c: get_thresholds([r[c] for r in data_rows if r[c] != ""]) for c in range(3, l_col)}
    for i, row in enumerate(data_rows):
        row_idx, row_formats = 81 + i, []
        for c in range(3, l_col):
            val = row[c]
            if val == "": # 空白セルを薄グレーに
                row_formats.append({"userEnteredFormat": {"backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}}})
            else:
                color, bold = get_surgical_format(val, col_thresholds.get(c))
                row_formats.append({"userEnteredFormat": {"textFormat": {"foregroundColor": color, "bold": bold}}})
        reqs.append({"updateCells": {"range": {"sheetId": s_id, "startRowIndex": row_idx, "endRowIndex": row_idx + 1, "startColumnIndex": 3, "endColumnIndex": l_col},
                                     "rows": [{"values": row_formats}], "fields": "userEnteredFormat.textFormat,userEnteredFormat.backgroundColor"}})

    # [5] グラフ
    reqs.append({"addChart": {"chart": {"spec": {"title": "トレンド比較", "basicChart": {"chartType": "LINE", "legendPosition": "BOTTOM_LEGEND", "axis": [{"position": "BOTTOM_AXIS"}, {"position": "LEFT_AXIS", "viewWindowOptions": {"viewWindowMin": 95, "viewWindowMax": 110, "viewWindowMode": "EXPLICIT"}}],
        "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 80, "endRowIndex": l_row, "startColumnIndex": 0, "endColumnIndex": 1}]}}}],
        "series": [{"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 80, "endRowIndex": l_row, "startColumnIndex": l_col+1, "endColumnIndex": l_col+2}]}}, "color": {"blue": 1.0}},
                   {"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 80, "endRowIndex": l_row, "startColumnIndex": l_col+2, "endColumnIndex": l_col+3}]}}, "color": {"red": 1.0}},
                   {"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 80, "endRowIndex": l_row, "startColumnIndex": l_col+3, "endColumnIndex": l_col+4}]}}, "color": {"red": 0.8, "green": 0.8, "blue": 0.8}}]}},
        "position": {"overlayPosition": {"anchorCell": {"sheetId": s_id, "rowIndex": 34, "columnIndex": 0}, "widthPixels": 3200, "heightPixels": 450}}}}})
    
    doc.batch_update({"requests": reqs})
    print(f"   -> {conf['target_model']} 解析完了。")

async def main():
    print(f"\n--- Ver.5.95 起動 (Final Precision) ---")
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