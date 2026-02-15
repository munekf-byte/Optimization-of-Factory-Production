# --- VERSION: m_commander_v17_5_Auto_Floor_Expansion ---
import gspread
from gspread.exceptions import WorksheetNotFound
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import asyncio
import csv
import collections
import jpholiday
import re
import time
import json
import os

# ==========================================
# BLOCK: 1. 固定設定 ＆ 拠点定義
# ==========================================
JSON_KEY_FILE   = 'service_account.json'
LOCAL_DATABASE  = "/Users/macuser/Desktop/minrepo_project/minrepo_database.csv"
REGISTRY_FILE   = "tab_registry.json"

CONFIG_SHEET    = "【HQ】単店_個別機種_グループ比較"
TEMPLATE_SHEET  = "TEMPLATE_SINGLE_v2"
INDEX_SHEET     = "機種目録"

NODES = [
    {
        "owner": "PM本体",
        "spreadsheet_id": "1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I",
        "allowed_stores": None,
        "expire_minutes": 1440
    },
    {
        "owner": "友人A",
        "spreadsheet_id": "1IWSE0oskQGkWXNJMc9oSUgW34l1q0A0JxVuGGcVooJc",
        "allowed_stores": ["ピーアーク北千住SSS", "エムディー目黒"],
        "expire_minutes": 30
    }
]

def hex_to_rgb(hex_str):
    hex_str = hex_str.lstrip('#')
    return {"red": int(hex_str[0:2], 16)/255.0, "green": int(hex_str[2:4], 16)/255.0, "blue": int(hex_str[4:6], 16)/255.0}

# ==========================================
# BLOCK: 2. 管理台帳 ＆ 補助ロジック
# ==========================================
def load_registry():
    if os.path.exists(REGISTRY_FILE):
        try:
            with open(REGISTRY_FILE, "r") as f: return json.load(f)
        except: return {}
    return {}

def save_registry(reg):
    with open(REGISTRY_FILE, "w") as f: json.dump(reg, f)

def record_tab_birth(ss_id, tab_name):
    reg = load_registry()
    if ss_id not in reg: reg[ss_id] = {}
    reg[ss_id][tab_name] = datetime.now().isoformat()
    save_registry(reg)

def get_rank_v17(model_data, p_dates):
    if not p_dates: return [], []
    u_stats = collections.defaultdict(list)
    for d in p_dates:
        if d in model_data:
            for u, v in model_data[d].items(): u_stats[u].append(v['diff'])
    res = [(u, int(sum(v)/len(v))) for u, v in u_stats.items() if v]
    sorted_res = sorted(res, key=lambda x: x[1], reverse=True)
    return sorted_res[:5], sorted_res[-5:][::-1]

def detect_periods_v17(model_data, sorted_dates):
    if not sorted_dates: return []
    first_d = sorted_dates[0] if sorted_dates else None
    if not first_d: return []
    prev_units = set(model_data[first_d].keys()) if model_data.get(first_d) else set()
    break_points = []
    for d in sorted_dates:
        curr_units = set(model_data[d].keys())
        if curr_units and curr_units != prev_units:
            break_points.append(d); prev_units = curr_units
    if not break_points:
        n = len(sorted_dates)
        if n >= 3: break_points = [sorted_dates[n//3], sorted_dates[2*n//3]]
    periods, start_idx = [], 0
    for bp in break_points + [None]:
        if bp:
            try:
                end_idx = sorted_dates.index(bp)
                periods.append(sorted_dates[start_idx:end_idx]); start_idx = end_idx
            except: pass
        else: periods.append(sorted_dates[start_idx:])
    return [p for p in periods if p][:3]

# ==========================================
# BLOCK: 3. UIステータス制御
# ==========================================
def set_status_lamp(doc, text, color_hex):
    try:
        ws = doc.worksheet(CONFIG_SHEET)
        req = {"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": 4, "endRowIndex": 5, "startColumnIndex": 3, "endColumnIndex": 4},
                "cell": {"userEnteredFormat": {"backgroundColor": hex_to_rgb(color_hex), "horizontalAlignment": "CENTER", "textFormat": {"bold": True}}, "userEnteredValue": {"stringValue": text}},
                "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,textFormat),userEnteredValue"}}
        doc.batch_update({"requests": [req]})
    except: pass

def set_mega_signal(doc, text, color_hex, font_size=18):
    try:
        ws = doc.worksheet(CONFIG_SHEET)
        req = {"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": 8, "endRowIndex": 13, "startColumnIndex": 3, "endColumnIndex": 4},
                "cell": {"userEnteredFormat": {"backgroundColor": hex_to_rgb(color_hex), "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE", "textFormat": {"bold": True, "fontSize": font_size}}, "userEnteredValue": {"stringValue": text}},
                "fields": "userEnteredFormat,userEnteredValue"}}
        doc.batch_update({"requests": [req]})
    except: pass

# ==========================================
# BLOCK: 4. メイン分析エンジン (v17.5 自動拡幅 ＆ トリミング)
# ==========================================
async def execute_single_analysis(target_doc, conf, store_master_ma30):
    m_clean = re.sub(r'スマスロ|パチスロ|\[.*?\]|^[LSP e]\s*|^\s+|\s+$', '', conf['target_model']).strip()
    target_short = m_clean[:4]
    match = re.search(r'([0-9]+|V|ZERO|覚醒|編|祭)$', m_clean)
    if match: target_short += match.group(1)
    
    print(f"   > [{conf['owner']}] 解析中: {target_short}")
    dow_names = ["月", "火", "水", "木", "金", "土", "日"]
    
    unit_app, raw_data = collections.defaultdict(list), []
    store_daily = collections.defaultdict(lambda: {'diff': 0, 'games': 0})
    target_compare = re.sub(r'スマスロ|パチスロ|\[.*?\]|^\s+|\s+$', '', conf['target_model']).strip()

    with open(LOCAL_DATABASE, mode='r', encoding='utf-8-sig') as f:
        reader = csv.reader(f); next(reader, None)
        for row in reader:
            if len(row) < 6: continue
            d_date, d_store, d_model, d_unit, d_diff, d_games = [c.strip() for c in row]
            if conf['store'] not in d_store: continue
            dt = datetime.strptime(d_date, "%Y/%m/%d")
            store_daily[d_date]['diff'] += int(d_diff); store_daily[d_date]['games'] += int(d_games)
            row_model_clean = re.sub(r'スマスロ|パチスロ|\[.*?\]|^\s+|\s+$', '', d_model).strip()
            if row_model_clean == target_compare and conf['start_date'] <= dt <= conf['end_date']:
                unit_app[int(d_unit)].append(dt); raw_data.append({'date': d_date, 'unit': int(d_unit), 'diff': int(d_diff), 'games': int(d_games)})

    if not raw_data: return
    valid_units = sorted([u for u, dts in unit_app.items() if any((sorted(dts)[i+2]-sorted(dts)[i]).days <= 4 for i in range(len(dts)-2))])
    if not valid_units: return

    model_data, unit_hist = collections.defaultdict(dict), collections.defaultdict(list)
    payout_h, games_h, dow_stats, digit_stats = [], [], collections.defaultdict(list), collections.defaultdict(list)
    all_d, all_g = [], []
    target_dates = sorted(list(set(r['date'] for r in raw_data)))

    for i, d_str in enumerate(target_dates):
        day_units = [r for r in raw_data if r['date'] == d_str and r['unit'] in valid_units]
        if not day_units: continue
        t_d, t_g = sum(r['diff'] for r in day_units), sum(r['games'] for r in day_units)
        u_cnt = len(day_units)
        payout_h.append(((t_d + t_g*3)/(max(1,t_g)*3)*100) if t_g > 0 else 100.0); games_h.append(t_g / max(1, u_cnt))
        for r in day_units:
            model_data[d_str][r['unit']] = {'diff': r['diff'], 'games': r['games']}
            unit_hist[r['unit']].append(r['diff']); all_d.append(r['diff']); all_g.append(r['games'])
        dt = datetime.strptime(d_str, "%Y/%m/%d"); avg_d = t_d / max(1, u_cnt)
        dow_stats[dt.weekday()].append(avg_d); digit_stats[dt.day % 10].append(avg_d)

    periods = detect_periods_v17(model_data, target_dates)
    p_res = [{'dates': p, 'rank': get_rank_v17(model_data, p)} for p in periods]
    total_ranks = get_rank_v17(model_data, target_dates)

    tab_name = f"{conf['store'][:4]}_{target_short}_{target_dates[-1].replace('/','')[4:]}"
    try: target_doc.del_worksheet(target_doc.worksheet(tab_name))
    except: pass
    ws = target_doc.duplicate_sheet(target_doc.worksheet(TEMPLATE_SHEET).id, insert_sheet_index=len(target_doc.worksheets()), new_sheet_name=tab_name)
    s_id = ws.id
    target_doc.batch_update({"requests": [{"updateSheetProperties": {"properties": {"sheetId": s_id, "hidden": False}, "fields": "hidden"}}]})
    
    # 【重要：拡幅】書き込み前にDZ列(130列目)を確実に確保する
    ws.resize(rows=1100, cols=150)
    record_tab_birth(target_doc.id, tab_name)

    # --- STEP 4: Dashboard 外科的精密書き込み ---
    updates = [
        {'range': 'B1', 'values': [[conf['store']]]}, {'range': 'B2', 'values': [[conf['target_model']]]},
        {'range': 'B4', 'values': [[target_dates[0]]]}, {'range': 'D4', 'values': [[target_dates[-1]]]},
        {'range': 'G3', 'values': [[int(sum(all_d)/max(1, len(all_d)))]]}, {'range': 'J3', 'values': [[int(sum(all_g)/max(1, len(all_g)))]]},
        {'range': 'M3', 'values': [[round(((sum(all_g)*3+sum(all_d))/(max(1,sum(all_g))*3)*100), 1)]]},
        {'range': 'B8:B14', 'values': [[int(sum(dow_stats[i])/max(1, len(dow_stats[i])))] for i in range(7)]},
        {'range': 'E8:E17', 'values': [[int(sum(digit_stats[i])/max(1, len(digit_stats[i])))] for i in range(10)]}
    ]
    p_rows = []
    for i, p in enumerate(p_res):
        p_d_l = [model_data[d] for d in p['dates'] if d in model_data]
        p_ds, p_gs, p_uc = sum(sum(u['diff'] for u in day.values()) for day in p_d_l), sum(sum(u['games'] for u in day.values()) for day in p_d_l), sum(len(day) for day in p_d_l)
        p_rows.append([f"{p['dates'][0]}〜{p['dates'][-1]}", "", "", int(p_ds/max(1,p_uc)), round(((p_gs*3+p_ds)/(max(1,p_gs)*3)*100),1), int(p_gs/max(1,p_uc))])
    updates.append({'range': 'H9:M11', 'values': p_rows})
    
    all_target_ranks = [total_ranks] + [r['rank'] for r in p_res]
    for ci, (best, worst) in enumerate(all_target_ranks):
        if ci > 3: break
        c_name = ['A','D','G','J'][ci]
        v_name = ['B','E','H','K'][ci]
        updates.append({'range': f'{c_name}22:{c_name}26', 'values': [[f"{r[0]}番台"] for r in best]})
        updates.append({'range': f'{v_name}22:{v_name}26', 'values': [[r[1]] for r in best]})
        updates.append({'range': f'{c_name}28:{c_name}32', 'values': [[f"{r[0]}番台"] for r in worst]})
        updates.append({'range': f'{v_name}28:{v_name}32', 'values': [[r[1]] for r in worst]})
    
    ws.batch_update(updates)

    # STEP 5: 個別サマリー ＆ データ倉庫
    valid_units_final = sorted(list(unit_hist.keys()))
    summ = [["" for _ in range(len(valid_units_final))] for _ in range(6)]
    for i, u in enumerate(valid_units_final):
        d_a = unit_hist[u]; days = len(d_a)
        for pi in range(3):
            if pi < len(periods):
                pdif = [model_data[d][u]['diff'] for d in periods[pi] if d in model_data and u in model_data[d]]
                summ[pi][i] = int(sum(pdif)/max(1, len(pdif))) if pdif else 0
        summ[3][i] = int(sum(d_a)/max(1, days))
        summ[4][i] = round(len([v for v in d_a if v>=5000])/max(1, days)*100, 1)
        summ[5][i] = round(len([v for v in d_a if v>=10000])/max(1, days)*100, 1)
    
    ws.update(values=summ, range_name=f'I80:{gspread.utils.rowcol_to_a1(85, 9+len(valid_units_final)-1)}')
    ws.update(values=[[f"{u}番" for u in valid_units_final]], range_name=f'I86:{gspread.utils.rowcol_to_a1(86, 9+len(valid_units_final)-1)}')

    d_rows, c_rows = [], []
    for i, d_str in enumerate(target_dates):
        day_u = model_data.get(d_str, {}); u_cnt = len(day_u)
        if u_cnt == 0: continue
        t_d, t_g = sum(x['diff'] for x in day_u.values()), sum(x['games'] for x in day_u.values())
        w7, w30 = max(0, i-6), max(0, i-29)
        ma7r, ma30r = sum(payout_h[w7:i+1])/len(payout_h[w7:i+1]), sum(payout_h[w30:i+1])/len(payout_h[w30:i+1])
        s_ma30 = store_master_ma30.get(d_str, 100.0)
        ma7g, ma30g = sum(games_h[w7:i+1])/len(games_h[w7:i+1]), sum(games_h[w30:i+1])/len(games_h[w30:i+1])
        sticky = round(len([x for x in day_u.values() if x['games']>=5000 and x['diff']>0])/max(1, u_cnt)*100, 1)
        row = [d_str, ["月","火","水","木","金","土","日"][datetime.strptime(d_str, "%Y/%m/%d").weekday()], "", t_d, int(t_d/max(1, u_cnt)), int(t_g/max(1, u_cnt)), round((t_g*3+t_d)/(max(1,t_g)*3)*100,1), sticky]
        for u in valid_units_final: row.append(day_u[u]['diff'] if u in day_u else "")
        d_rows.append(row); c_rows.append([round(s_ma30, 2), round(ma7r, 2), round(ma30r, 2), round(ma7g, 2), round(ma30g, 2)])

    ws.update(values=d_rows, range_name=f'A87:{gspread.utils.rowcol_to_a1(87+len(d_rows)-1, 8+len(valid_units_final))}')
    ws.update(values=c_rows, range_name='DZ87')

    # STEP 6: グラフ (1600px軽量化) ＆ トリミング
    meta = target_doc.fetch_sheet_metadata(); charts = next(s for s in meta['sheets'] if s['properties']['sheetId'] == s_id).get('charts', [])
    reqs = [{"deleteEmbeddedObject": {"objectId": c['chartId']}} for c in charts]
    lr, dz = len(d_rows) + 87, 129
    
    reqs.append({"addChart": {"chart": {"spec": {"title": "【機械割】トレンド(灰:店 青:MA7 赤:MA30)", "basicChart": {"chartType": "LINE", "legendPosition": "BOTTOM_LEGEND", "axis": [{"position": "BOTTOM_AXIS"}, {"position": "LEFT_AXIS", "viewWindowOptions": {"viewWindowMin": 90, "viewWindowMax": 110, "viewWindowMode": "EXPLICIT"}}],
        "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 86, "endRowIndex": lr, "startColumnIndex": 0, "endColumnIndex": 1}]}}}],
        "series": [
            {"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 86, "endRowIndex": lr, "startColumnIndex": dz, "endColumnIndex": dz+1}]}}, "color": hex_to_rgb("#333333"), "lineStyle": {"width": 2}},
            {"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 86, "endRowIndex": lr, "startColumnIndex": dz+1, "endColumnIndex": dz+2}]}}, "color": {"blue": 1.0}, "lineStyle": {"width": 2}},
            {"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 86, "endRowIndex": lr, "startColumnIndex": dz+2, "endColumnIndex": dz+3}]}}, "color": {"red": 1.0}, "lineStyle": {"width": 3}}
        ]}}, "position": {"overlayPosition": {"anchorCell": {"sheetId": s_id, "rowIndex": 34, "columnIndex": 0}, "widthPixels": 1600, "heightPixels": 450}}}}})
    reqs.append({"addChart": {"chart": {"spec": {"title": "【平均G数】稼働トレンド", "basicChart": {"chartType": "LINE", "legendPosition": "BOTTOM_LEGEND", "axis": [{"position": "BOTTOM_AXIS"}, {"position": "LEFT_AXIS"}],
        "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 86, "endRowIndex": lr, "startColumnIndex": 0, "endColumnIndex": 1}]}}}],
        "series": [
            {"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 86, "endRowIndex": lr, "startColumnIndex": dz+3, "endColumnIndex": dz+4}]}}, "color": {"red": 0.2, "green": 0.8, "blue": 0.2}, "lineStyle": {"width": 3}},
            {"series": {"sourceRange": {"sources": [{"sheetId": s_id, "startRowIndex": 86, "endRowIndex": lr, "startColumnIndex": dz+4, "endColumnIndex": dz+5}]}}, "color": {"red": 0.4, "green": 0.4, "blue": 0.4}, "lineStyle": {"width": 3}}
        ]}}, "position": {"overlayPosition": {"anchorCell": {"sheetId": s_id, "rowIndex": 57, "columnIndex": 0}, "widthPixels": 1600, "heightPixels": 450}}}}})
    
    cal_rows = []
    for d_str in target_dates:
        dt = datetime.strptime(d_str, "%Y/%m/%d")
        color = {"red": 1, "green": 0, "blue": 0} if dt.weekday()==6 or jpholiday.is_holiday(dt) else ({"red": 0, "green": 0, "blue": 1} if dt.weekday()==5 else {"red": 0, "green": 0, "blue": 0})
        cal_rows.append({"values": [{"userEnteredFormat": {"textFormat": {"foregroundColor": color}}}, {"userEnteredFormat": {"textFormat": {"foregroundColor": color}}}]})
    reqs.append({"updateCells": {"range": {"sheetId": s_id, "startRowIndex": 86, "endRowIndex": 86+len(cal_rows), "startColumnIndex": 0, "endColumnIndex": 2}, "rows": cal_rows, "fields": "userEnteredFormat.textFormat.foregroundColor"}})

    # 【軽量化】最終トリミング (実データ行 + 30 / 135列にピタッと合わせる)
    reqs.append({"updateSheetProperties": {"properties": {"sheetId": s_id, "gridProperties": {"rowCount": lr + 30, "columnCount": 135}}, "fields": "gridProperties.rowCount,gridProperties.columnCount"}})

    target_doc.batch_update({"requests": reqs})
    print(f"   -> [{conf['owner']}] 納品完了: {tab_name}")
    await asyncio.sleep(25)

# ==========================================
# BLOCK: 5. 巡回ロジック
# ==========================================
async def sync_node_all(node):
    doc = node['doc']
    try:
        set_status_lamp(doc, "● 同期中...", "#00ffff")
        unique_stores = set()
        with open(LOCAL_DATABASE, mode='r', encoding='utf-8-sig') as f:
            reader = csv.reader(f); next(reader, None)
            for row in reader:
                if node['allowed_stores'] is None or row[1] in node['allowed_stores']: unique_stores.add(row[1])
        stores = sorted(list(unique_stores))
        idx_ws = doc.worksheet(INDEX_SHEET); idx_ws.batch_clear(['A:B'])
        idx_ws.update(values=[["店舗リスト"]] + [[s] for s in stores], range_name='A1')
        
        model_stats = collections.defaultdict(lambda: {'last7_g': 0, 'all_g': 0, 'all_d': 0, 'is_rem': False})
        cur_s = doc.worksheet(CONFIG_SHEET).acell('B5').value
        if cur_s:
            with open(LOCAL_DATABASE, mode='r', encoding='utf-8-sig') as f:
                reader = csv.reader(f); next(reader, None)
                for row in reader:
                    if cur_s in row[1]:
                        model_stats[row[2]]['all_g'] += int(row[5]); model_stats[row[2]]['all_d'] += 1
                        if "[撤去]" in row[2]: model_stats[row[2]]['is_rem'] = True
            f_list = sorted(model_stats.keys(), key=lambda m: (not model_stats[m]['is_rem'], model_stats[m]['all_g']/max(1,model_stats[m]['all_d'])), reverse=True)
            idx_ws.update(values=[["店舗別機種リスト"]] + [[m] for m in f_list], range_name='B1')
            
        conf_ws = doc.worksheet(CONFIG_SHEET)
        reqs = [
            {"setDataValidation": {"range": {"sheetId": conf_ws.id, "startRowIndex": 4, "endRowIndex": 5, "startColumnIndex": 1, "endColumnIndex": 2}, "rule": {"condition": {"type": "ONE_OF_RANGE", "values": [{"userEnteredValue": f"='{INDEX_SHEET}'!$A$2:$A$1000"}]}, "showCustomUi": True, "strict": False}}},
            {"setDataValidation": {"range": {"sheetId": conf_ws.id, "startRowIndex": 8, "endRowIndex": 13, "startColumnIndex": 1, "endColumnIndex": 2}, "rule": {"condition": {"type": "ONE_OF_RANGE", "values": [{"userEnteredValue": f"='{INDEX_SHEET}'!$B$2:$B$1000"}]}, "showCustomUi": True, "strict": True}}}
        ]
        doc.batch_update({"requests": reqs})
        set_status_lamp(doc, "同期完了 (選択可)", "#00ff00")
    except Exception as e: print(f"Sync Error: {e}")

async def get_store_master_ma30(store_name):
    daily_raw = collections.defaultdict(lambda: {'diff': 0, 'games': 0})
    with open(LOCAL_DATABASE, mode='r', encoding='utf-8-sig') as f:
        reader = csv.reader(f); next(reader, None)
        for row in reader:
            if store_name in row[1]: daily_raw[row[0]]['diff'] += int(row[4]); daily_raw[row[0]]['games'] += int(row[5])
    sorted_days = sorted(daily_raw.keys())
    p_h = [(daily_raw[d]['games']*3 + daily_raw[d]['diff'])/(max(1,daily_raw[d]['games'])*3)*100 for d in sorted_days]
    return {d: sum(p_h[max(0, i-29):i+1])/max(1, len(p_h[max(0, i-29):i+1])) for i, d in enumerate(sorted_days)}

async def cleanup_patrol(doc, node):
    reg = load_registry(); ss_id = doc.id
    if ss_id not in reg: return
    now = datetime.now()
    for tab_name, birth_str in list(reg[ss_id].items()):
        elapsed = (now - datetime.fromisoformat(birth_str)).total_seconds() / 60
        if elapsed >= node['expire_minutes'] and elapsed > 5:
            try:
                target_ws = doc.worksheet(tab_name)
                if "保存" not in target_ws.title: doc.del_worksheet(target_ws)
            except: pass
            del reg[ss_id][tab_name]
    save_registry(reg)

# ==========================================
# BLOCK: 6. 統合メインループ
# ==========================================
async def main():
    print(f"\n--- Ver.17.5 起動 (Auto Floor Expansion) ---")
    creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_FILE, ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
    gc = gspread.authorize(creds)

    for n in NODES:
        try:
            n['doc'] = gc.open_by_key(n['spreadsheet_id']); n['last_s'] = "INIT"; await sync_node_all(n)
        except Exception as e: print(f"Init Error [{n['owner']}]: {e}")

    last_fin = None
    while True:
        for n in NODES:
            try:
                doc, owner = n['doc'], n['owner']
                print(f"\r[{datetime.now().strftime('%H:%M:%S')}] {owner} 監視中...", end="")
                conf_ws = doc.worksheet(CONFIG_SHEET)
                cur_s = conf_ws.acell('B5').value.strip() if conf_ws.acell('B5').value else ""
                
                if cur_s == "" and n['last_s'] != "":
                    set_status_lamp(doc, "同期待ち", "#eeeeee"); n['last_s'] = ""
                elif cur_s != "" and cur_s != n['last_s']:
                    print(f"\n[{owner}] 店舗変更検知: {cur_s}")
                    await sync_node_all(n); n['last_s'] = cur_s

                vals = conf_ws.get_all_values()
                cmd = vals[8][2] if len(vals) > 8 and len(vals[8]) > 2 else ""
                
                if "実行" in cmd:
                    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 指令受理: {owner}")
                    set_mega_signal(doc, "⚠️ 解析中...", "#00ffff", font_size=20)
                    conf_ws.update(values=[["解析中..."] for _ in range(5)], range_name='B9:B13')
                    conf_ws.update_acell('C9', "処理中")
                    s_master = await get_store_master_ma30(cur_s)
                    s_dt = datetime.strptime(vals[5][1], "%Y/%m/%d") if len(vals)>5 and vals[5][1] else datetime(2000,1,1)
                    e_dt = datetime.strptime(vals[6][1], "%Y/%m/%d") if len(vals)>6 and vals[6][1] else datetime(2099,12,31)
                    targets = [v[1] for v in vals[8:13] if len(v)>1 and v[1] and "解析中" not in v[1]]
                    
                    for i, m in enumerate(targets):
                        set_mega_signal(doc, f"● {i+1}/{len(targets)} {m[:4]} 解析中", "#00ffff", font_size=18)
                        await execute_single_analysis(n['doc'], {"owner":owner, "store":cur_s, "target_model":m, "start_date":s_dt, "end_date":e_dt}, s_master)
                    
                    conf_ws.update_acell('B5', ""); n['last_s'] = ""
                    conf_ws.update(values=[[""] for _ in range(5)], range_name='B9:B13')
                    set_status_lamp(doc, "同期待ち", "#eeeeee")
                    set_mega_signal(doc, "待機中 (完了)", "#00ff00", font_size=18)
                    conf_ws.update_acell('C9', "完了"); last_fin = datetime.now()
                else:
                    if last_fin and (datetime.now()-last_fin).total_seconds() > 180:
                        set_mega_signal(doc, "待機中", "#ffff00"); last_fin = None
                await cleanup_patrol(doc, n)
            except Exception as e: print(f"\nError [{owner}]: {e}")
        await asyncio.sleep(5)

if __name__ == "__main__": asyncio.run(main())