# --- VERSION: m_chronicler_v3_1_Veterans_Precision ---
import gspread
from gspread.exceptions import WorksheetNotFound
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import csv
import collections
import os
import math

# ==========================================
# BLOCK: 1. 固定設定
# ==========================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_KEY_FILE   = os.path.join(BASE_DIR, 'service_account.json')
SPREADSHEET_KEY = "1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I"
LOCAL_DATABASE  = "/Users/macuser/Desktop/minrepo_project/minrepo_database.csv"

# 熟練機（Veteran）の入隊基準
VETERAN_DAYS = 90
VETERAN_UNITS = 4

def calculate_payout(diff, games):
    if games <= 0: return 100.0
    return ((games * 3 + diff) / (max(1, games) * 3)) * 100

def check_3_of_5(sorted_dts):
    if len(sorted_dts) < 3: return False
    for i in range(len(sorted_dts)-2):
        if (sorted_dts[i+2] - sorted_dts[i]).days <= 4: return True
    return False

# ==========================================
# BLOCK: 2. 熟練機・精密分析エンジン
# ==========================================
def run_veteran_analysis_v3_1():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] ⏳ 全データロード及び精密熟練判定開始...")
    db = collections.defaultdict(lambda: collections.defaultdict(lambda: collections.defaultdict(dict)))
    
    with open(LOCAL_DATABASE, mode='r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 6: continue
            try:
                d_date, d_store, d_model, d_unit, d_diff, d_games = [c.strip() for c in row]
                dt = datetime.strptime(d_date, "%Y/%m/%d")
                db[d_store][d_model][int(d_unit)][dt] = {'diff': int(d_diff), 'games': int(d_games)}
            except: continue

    results = []

    for store, models in db.items():
        all_store_dates = sorted(list(set(dt for m in models.values() for u in m.values() for dt in u.keys())))
        if not all_store_dates: continue
        latest_date = all_store_dates[-1]

        # 店舗平均（看板/補欠判定用）
        st_diff = sum(u_d['diff'] for m in models.values() for u in m.values() for u_d in u.values())
        st_games = sum(u_d['games'] for m in models.values() for u in m.values() for u_d in u.values())
        store_base = calculate_payout(st_diff, st_games)

        for model, units in models.items():
            unit_count = len([u_id for u_id, hist in units.items() if latest_date in hist])
            m_all_dates = sorted(list(set(dt for u in units.values() for dt in u.keys())))
            installation_days = (m_all_dates[-1] - m_all_dates[0]).days if m_all_dates else 0
            
            # 熟練判定
            is_veteran = (installation_days >= VETERAN_DAYS and unit_count >= VETERAN_UNITS)
            
            model_history = collections.defaultdict(lambda: {'diff': 0, 'games': 0})
            for u_id, hist in units.items():
                if check_3_of_5(sorted(list(hist.keys()))):
                    for d, val in hist.items():
                        model_history[d]['diff'] += val['diff']; model_history[d]['games'] += val['games']
            
            if not model_history: continue
            
            m_payouts_raw = [calculate_payout(model_history[d]['diff'], model_history[d]['games']) for d in m_all_dates if d in model_history]
            model_base = sum(m_payouts_raw) / len(m_payouts_raw) if m_payouts_raw else 100.0
            handle_type = "看板" if model_base > store_base else "補欠"

            target_point = "N/A"
            reversal_win_rate = "N/A"
            trial_count = "N/A"

            if is_veteran:
                all_payouts_filled = [calculate_payout(model_history[d]['diff'], model_history[d]['games']) if d in model_history else 100.0 for d in all_store_dates]
                
                bin_stats = collections.defaultdict(lambda: {"wins": 0, "total": 0})
                for i in range(30, len(all_payouts_filled)-3):
                    ma7 = sum(all_payouts_filled[i-6:i+1]) / 7
                    ma30 = sum(all_payouts_filled[i-29:i+1]) / 30
                    divergence = ma7 - ma30
                    if divergence < 0:
                        div_bin = math.floor(divergence)
                        f_pay = sum(all_payouts_filled[i+1:i+4]) / 3
                        bin_stats[div_bin]["total"] += 1
                        if f_pay > ma30: bin_stats[div_bin]["wins"] += 1
                
                best_bin = None
                max_wr = 0
                count_at_best = 0
                for b, s in bin_stats.items():
                    wr = s["wins"] / s["total"]
                    if s["total"] >= 5 and wr > max_wr:
                        max_wr = wr
                        best_bin = b
                        count_at_best = s["total"]
                
                if best_bin is not None:
                    target_point = f"{best_bin}%"
                    reversal_win_rate = f"{max_wr*100:.1f}%"
                    trial_count = count_at_best

            results.append([
                store, model, "熟練" if is_veteran else "一般", 
                handle_type, f"{model_base:.2f}%", unit_count, 
                f"{installation_days}日", target_point, reversal_win_rate, trial_count
            ])

    return results

# ==========================================
# BLOCK: 3. 納品
# ==========================================
def deliver_veteran_tactics_v3_1(doc, data):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 🛠 スプレッドシート納品中...")
    try:
        ws = doc.worksheet("Sentinel_Veteran_Tactics")
        ws.clear()
    except WorksheetNotFound:
        ws = doc.add_worksheet("Sentinel_Veteran_Tactics", 5000, 11)
    
    header = [["店舗名", "機種名", "熟練判定", "区分", "機種平均割", "設置台数", "設置期間", "反転臨界点", "反転成功率", "試行回数"]]
    # 熟練機を優先し、反転成功率でソート
    data.sort(key=lambda x: (x[2] == "一般", x[8] == "N/A", x[8]), reverse=False)
    ws.update(values=header + data, range_name='A1')
    print("【Sentinel_Veteran_Tactics】の精密更新が完了しました。")

if __name__ == "__main__":
    creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_FILE, ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
    gc = gspread.authorize(creds); doc = gc.open_by_key(SPREADSHEET_KEY)
    results = run_veteran_analysis_v3_1()
    deliver_veteran_tactics_v3_1(doc, results)