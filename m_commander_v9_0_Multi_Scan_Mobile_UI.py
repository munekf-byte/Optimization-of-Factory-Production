# --- VERSION: m_commander_v9_0_Multi_Scan_Mobile_UI ---
import gspread
from gspread.exceptions import WorksheetNotFound
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import asyncio
import csv
import collections
import jpholiday
import re

# ==========================================
# BLOCK: 1. 固定設定
# ==========================================
SPREADSHEET_KEY = "1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I"
CONFIG_SHEET    = "分析設定"
INDEX_SHEET     = "機種目録" 
LOCAL_DATABASE  = "/Users/macuser/Desktop/minrepo_project/minrepo_database.csv"

def hex_to_rgb(hex_str):
    hex_str = hex_str.lstrip('#')
    return {"red": int(hex_str[0:2], 16)/255.0, "green": int(hex_str[2:4], 16)/255.0, "blue": int(hex_str[4:6], 16)/255.0}

# ==========================================
# BLOCK: 2. 略称生成ロジック
# ==========================================
def generate_short_tab_name(store, model, date_str):
    # 店名：最初の4文字
    s_short = store[:4]
    
    # 機種名のクリーンアップ
    m_clean = re.sub(r'\[撤去\]|Lパチスロ|Sパチスロ|スマスロ|パチスロ', '', model).strip()
    
    # シリーズ識別の抽出（末尾の数字や記号）
    series_id = ""
    match = re.search(r'([0-9]+|V|ZERO|覚醒|編|祭)$', m_clean)
    if match:
        series_id = match.group(1)
        m_base = m_clean[:-len(series_id)].strip()
    else:
        m_base = m_clean
    
    # 本体：最初の4文字 + 識別キー
    m_short = m_base[:4] + series_id
    
    # 日付：MMDD
    d_short = date_str.replace('/', '')[4:] if '/' in date_str else date_str[-4:]
    
    return f"{s_short}_{m_short}_{d_short}"

# ==========================================
# BLOCK: 3. UIステータス管理
# ==========================================
def set_status_ui(doc, text, color_hex):
    try:
        conf_ws = doc.worksheet(CONFIG_SHEET)
        # 背景色とテキストを一括更新 (C8セルをターゲットとする)
        req = {
            "repeatCell": {
                "range": {"sheetId": conf_ws.id, "startRowIndex": 7, "endRowIndex": 8, "startColumnIndex": 2, "endColumnIndex": 3},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": hex_to_rgb(color_hex),
                        "horizontalAlignment": "CENTER",
                        "textFormat": {"bold": True}
                    },
                    "userEnteredValue": {"stringValue": text}
                },
                "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,textFormat),userEnteredValue"
            }
        }
        doc.batch_update({"requests": [req]})
    except: pass

# ==========================================
# BLOCK: 4. 高度な分析ロジック (継承)
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
    return sorted(u_avgs, key=lambda x: x[1], reverse=True)[:5], sorted(u_avgs, key=lambda x: x[1])[:5][::-1]

def split_periods_3(model_data, sorted_dates):
    if not sorted_dates: return []
    first_date = sorted_dates[0]
    prev_units = set(model_data[first_date].keys()) if first_date in model_data else set()
    break_points = []
    for d in sorted_dates:
        curr_units = set(model_data[d].keys()) if d in model_data else set()
        if curr_units and curr_units != prev_units:
            break_points.append(d); prev_units = curr_units
    if not break_points:
        n = len(sorted_dates)
        if n >= 3: break_points = [sorted_dates[n//3], sorted_dates[2*n//3]]
    periods, start_idx = [], 0
    for bp in break_points + [None]:
        if bp:
            end_idx = sorted_dates.index(bp); periods.append(sorted_dates[start_idx:end_idx]); start_idx = end_idx
        else: periods.append(sorted_dates[start_idx:])
    return [p for p in periods if p][:3]

# ==========================================
# BLOCK: 5. メインエンジン
# ==========================================
async def execute_single_analysis(doc, conf):
    # --- STEP 1: データ抽出 ---
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

    # --- STEP 2: 集計 ---
    model_data, unit_history = collections.defaultdict(dict), collections.defaultdict(list)
    dow_stats, digit_stats = collections.defaultdict(list), collections.defaultdict(list)
    payout_h, store_payout_h = [], []
    all_diffs, all_games = [], []
    target_dates = sorted(list(set(r['date'] for r in raw_data)))
    
    for d_str in target_dates:
        day_units = [r for r in raw_data if r['date'] == d_str and r['unit'] in valid_units]
        t_d, t_g = sum(r['diff'] for r in day_units), sum(r['games'] for r in day_units)
        payout_h.append(((t_g*3+t_d)/(t_g*3)*100) if t_g > 0 else 100.0)
        for r in day_units:
            model_data[d_str][r['unit']] = {'diff': r['diff'], 'games': r['games']}
            unit_history[r['unit']].append(r['diff'])
            all_diffs.append(r['diff']); all_games.append(r['games'])
        s_d, s_g = store_daily_stats[d_str]['diff'], store_daily_stats[d_str]['games']
        store_payout_h.append(((s_g*3+s_d)/(s_g*3)*100) if s_g > 0 else 100.0)
        dt = datetime.strptime(d_str, "%Y/%m/%d")
        if day_units:
            avg_d = t_d / len(day_units)
            dow_stats[dt.weekday()].append(avg_d); digit_stats[dt.day % 10].append(avg_d)

    periods = split_periods_3(model_data, target_dates)
    total_best, total_worst = get_period_rankings_5(model_data, target_dates)
    p_res = [{'dates': p, 'best': get_period_rankings_5(model_data, p)[0], 'worst': get_period_rankings_5(model_data, p)[1]} for p in periods]

    # --- STEP 3: タブ命名 & シート建築 ---
    short_tab_name = generate_short_tab_name(conf['store'], conf['target_model'], target_dates[-1])
    try: old = doc.worksheet(short_tab_name); doc.del_worksheet(old)
    except WorksheetNotFound: pass
    ws = doc.add_worksheet(title=short_tab_name, rows="2000", cols="200")
    s_id = ws.id

    # --- STEP 4: Dashboard & Data 流し込み (v8.2ロジック) ---
    avg_d_total = int(sum(all_diffs)/len(all_diffs)) if all_diffs else 0
    avg_g_total = int(sum(all_games)/len(all_games)) if all_games else 0
    avg_r_total = ((sum(all_games)*3+sum(all_diffs))/(sum(all_games)*3)*100) if sum(all_games)>0 else 100.0
    
    dash = [[""] * 15 for _ in range(33)]
    dash[0][0], dash[0][1] = "【レポート】", conf['store']
    dash[1][0], dash[1][1] = "機種名▶️", conf['target_model']
    dash[1][6], dash[1][9], dash[1][12] = "🔽TOTAL台平均差枚", "🔽TOTAL台平均G数", "🔽TOTAL機械割"
    dash[2][6], dash[2][9], dash[2][12] = f"{avg_d_total}枚", f"{avg_g_total}G", f"{avg_r_total:.1f}%"
    dash[3][0], dash[3][1], dash[3][3] = "解析期間▶️", target_dates[0], target_dates[-1]
    
    dow_names = ["月", "火", "水", "木", "金", "土", "日"]
    for i in range(7): dash[7+i][0], dash[7+i][1] = dow_names[i], f"{int(sum(dow_stats[i])/len(dow_stats[i]))}枚" if dow_stats[i] else "0枚"
    for i in range(10): dash[7+i][3], dash[7+i][4] = f"末{i}", f"{int(sum(digit_stats[i])/len(digit_stats[i]))}枚" if digit_stats[i] else "0枚"
    
    dash[7][7], dash[7][10], dash[7][11], dash[7][12] = "＜期間＞", "＜差枚＞", "＜割＞", "＜G数＞"
    for i, p in enumerate(p_res):
        p_d = [model_data[d] for d in p['dates'] if d in model_data]
        if not p_d: continue
        p_ds, p_gs, p_uc = sum(sum(u['diff'] for u in day.values()) for day in p_d), sum(sum(u['games'] for u in day.values()) for day in p_d), sum(len(day) for day in p_d)
        dash[8+i][6], dash[8+i][7] = ["前期","中期","後期"][i], f"{p['dates'][0]}〜{p['dates'][-1]}"
        dash[8+i][10], dash[8+i][11], dash[8+i][12] = f"{int(p_ds/p_uc) if p_uc>0 else 0}枚", f"{(p_gs*3+p_ds)/(p_gs*3)*100 if p_gs>0 else 100.0:.1f}%", f"{int(p_gs/p_uc) if p_uc>0 else 0}G"

    dash[18][0], dash[19][1], dash[19][4], dash[19][7], dash[19][10] = "➖個別台分析➖", "全期間", "前期", "中期", "後期"
    for col_idx, r in enumerate([(total_best, total_worst)] + [(p['best'], p['worst']) for p in p_res]):
        if col_idx > 3: break
        c_b = col_idx * 3
        dash[20][c_b], dash[26][c_b] = "👑BEST5", "💀WORST5"
        for j in range(5):
            if j < len(r[0]): dash[21+j][c_b], dash[21+j][c_b+1] = f"{r[0][j][0]}番台", f"{r[0][j][1]}枚"
            if j < len(r[1]): dash[27+j][c_b], dash[27+j][c_b+1] = f"{r[1][j][0]}番台", f"{r[1][j][1]}枚"
    ws.update(values=dash, range_name='A1')

    # --- STEP 5: 三期サマリー & データ倉庫 (A74〜) ---
    summary_header = [["" for _ in range(len(valid_units) + 8)] for _ in range(7)]
    labels = ["平均差枚(前期)", "平均差枚(中期)", "平均差枚(後期)", "TOTAL平均差枚", "5000枚突破率", "10000枚突破率", "台番表記"]
    for idx, lbl in enumerate(labels): summary_header[idx][6] = lbl 
    for i, u in enumerate(valid_units):
        c_idx = i + 8
        d_all = unit_history[u]; days = len(d_all)
        p_avgs = []
        for p_idx in range(3):
            if p_idx < len(periods):
                p_diffs = [model_data[d][u]['diff'] for d in periods[p_idx] if d in model_data and u in model_data[d]]
                p_avgs.append(f"{int(sum(p_diffs)/len(p_diffs)) if p_diffs else 0}枚")
            else: p_avgs.append("-")
        summary_header[0][c_idx], summary_header[1][c_idx], summary_header[2][c_idx] = p_avgs
        summary_header[3][c_idx] = f"{int(sum(d_all)/days) if days>0 else 0}枚"
        summary_header[4][c_idx] = f"{len([v for v in d_all if v>=5000])/days*100:.1f}%" if days>0 else "0.0%"
        summary_header[5][c_idx] = f"{len([v for v in d_all if v>=10000])/days*100:.1f}%" if days>0 else "0.0%"
        summary_header[6][c_idx] = f"{u}番"

    data_header = ["日付", "曜日", "イベントログ", "総計", "台平均", "平均G", "機械割", "粘り勝率"] + [f"{u}番" for u in valid_units]
    data_rows, calc_rows = [], []
    for i, d_str in enumerate(target_dates):
        u_cnt = len(model_data[d_str])
        if u_cnt == 0: continue
        t_d, t_g = sum(u['diff'] for u in model_data[d_str].values()), sum(u['games'] for u in model_data[d_str].values())
        w7, w30 = max(0, i-6), max(0, i-29)
        ma7 = sum(payout_h[w7:i+1])/len(payout_h[w7:i+1]); ma30 = sum(payout_h[w30:i+1])/len(payout_h[w30:i+1]); s_ma30 = sum(store_payout_h[w30:i+1])/len(store_payout_h[w30:i+1])
        row = [d_str, dow_names[datetime.strptime(d_str, "%Y/%m/%d").weekday()], "", t_d, int(t_d/u_cnt), int(t_g/u_cnt), f"{(t_g*3+t_d)/(t_g*3)*100 if t_g>0 else 100.0:.1f}%", f"{(len([u for u in model_data[d_str].values() if u['games']>=5000 and u['diff']>0])/u_cnt*100):.1f}%"]
        for u in valid_units: row.append(model_data[d_str][u]['diff'] if u in model_data[d_str] else "")
        data_rows.append(row); calc_rows.append([ma7, ma30, s_ma30])

    ws.update(values=summary_header, range_name='A74')
    ws.update(values=[data_header] + data_rows, range_name='A80')
    
    # 隔離データ (+20列右)
    calc_start_col_idx = len(data_header) + 20
    def index_to_a1(idx):
        res = ""
        while idx >= 0: res = chr(idx % 26 + 65) + res; idx = idx // 26 - 1
        return res
    ws.update(values=calc_rows, range_name=f'{index_to_a1(calc_start_col_idx)}81')

    # --- STEP 6: グラフ & 装飾 ---
    l_row, l_col = len(data_rows) + 80, len(data_header)
    reqs = [{"updateDimensionProperties": {"range": {"sheetId": s_id, "dimension": "COLUMNS", "startIndex": 3, "endIndex": l_col}, "properties": {"pixelSize": 60}, "fields": "pixelSize"}}]
    reqs.append({"addChart": {"chart": {"spec": {"title": f"{conf['target_model']} トレンド", "basicChart": {"chartType": "LINE", "legendPosition": "BOTTOM_LEGEND", "axis": [{"position": "BOTTOM_AXIS"}, {"position": "LEFT_AXIS", "viewWindowOptions": {"viewWindowMin": 90, "viewWindowMax": 110, "viewWindowMode": "EXPLICIT"}}],
        "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 79, "endRowIndex": l_row, "startColumnIndex": 0, "endColumnIndex": 1}]}}}],
        "series": [{"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 80, "endRowIndex": l_row, "startColumnIndex": calc_start_col_idx, "endColumnIndex": calc_start_col_idx+1}]}}, "color": {"blue": 1.0}, "lineStyle": {"width": 2}},
                   {"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 80, "endRowIndex": l_row, "startColumnIndex": calc_start_col_idx+1, "endColumnIndex": calc_start_col_idx+2}]}}, "color": {"red": 1.0}, "lineStyle": {"width": 3}},
                   {"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 80, "endRowIndex": l_row, "startColumnIndex": calc_start_col_idx+2, "endColumnIndex": calc_start_col_idx+3}]}}, "color": {"red": 0.8, "green": 0.8, "blue": 0.8}, "lineStyle": {"width": 2}}]}},
        "position": {"overlayPosition": {"anchorCell": {"sheetId": s_id, "rowIndex": 34, "columnIndex": 0}, "widthPixels": 3200, "heightPixels": 450}}}}})
    
    for i, d_str in enumerate(target_dates):
        dt = datetime.strptime(d_str, "%Y/%m/%d")
        color = {"red": 1, "green": 0, "blue": 0} if dt.weekday()==6 or jpholiday.is_holiday(dt) else ({"red": 0, "green": 0, "blue": 1} if dt.weekday()==5 else {"red": 0, "green": 0, "blue": 0})
        reqs.append({"updateCells": {"range": {"sheetId": s_id, "startRowIndex": 80+i, "endRowIndex": 81+i, "startColumnIndex": 0, "endColumnIndex": 2}, "rows": [{"values": [{"userEnteredFormat": {"textFormat": {"foregroundColor": color}}}, {"userEnteredFormat": {"textFormat": {"foregroundColor": color}}}]}], "fields": "userEnteredFormat.textFormat.foregroundColor"}})
    doc.batch_update({"requests": reqs})
    print(f"   -> {conf['target_model']} タブ生成完了: {short_tab_name}")

async def sync_store_list(doc):
    unique_stores = set()
    with open(LOCAL_DATABASE, mode='r', encoding='utf-8-sig') as f:
        reader = csv.reader(f); next(reader, None) 
        for row in reader:
            if len(row) > 1: unique_stores.add(row[1])
    idx_ws = doc.worksheet(INDEX_SHEET); idx_ws.clear()
    idx_ws.update(values=[["店舗リスト(AutoSync)"]] + [[s] for s in sorted(list(unique_stores))], range_name='A1')

async def main():
    print(f"\n--- Ver.9.0 起動 (Multi-Scan & UI Status) ---")
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
    gc = gspread.authorize(creds); doc = gc.open_by_key(SPREADSHEET_KEY)
    await sync_store_list(doc)
    last_finish_time = None
    
    while True:
        try:
            conf_ws = doc.worksheet(CONFIG_SHEET); vals = conf_ws.get_all_values()
            store_name = vals[4][1]
            targets = [v[2] for v in vals[7:12] if v[2]] # C8:C12 の機種名を取得
            cmd = vals[7][3] # D8セルの「実行」コマンド
            
            # --- ステータス色の動的制御 ---
            now = datetime.now()
            if "実行" in cmd:
                set_status_ui(doc, "● 処理中", "#00ffff") # 水色
                for i, model in enumerate(targets):
                    progress_text = f"● {i+1}/{len(targets)} {model[:4]} 解析中"
                    set_status_ui(doc, progress_text, "#00ffff")
                    await execute_single_analysis(doc, {"store": store_name, "target_model": model})
                
                set_status_ui(doc, "待機中 (完了)", "#00ff00") # 緑色
                last_finish_time = datetime.now()
            else:
                # 5分間タイマー判定
                if last_finish_time and (now - last_finish_time).total_seconds() < 300:
                    pass # 緑色を維持
                else:
                    set_status_ui(doc, "待機中", "#ffff00") # 黄色
            
            print(f"\r[{datetime.now().strftime('%H:%M:%S')}] STAND BY ...", end="")
        except Exception as e: print(f"\nError: {e}"); await asyncio.sleep(5)
        await asyncio.sleep(15)

if __name__ == "__main__": asyncio.run(main())