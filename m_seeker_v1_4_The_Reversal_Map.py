# --- VERSION: m_seeker_v1_4_The_Reversal_Map ---
import gspread
from gspread.exceptions import WorksheetNotFound
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import csv
import collections
import os
import math
import time

# ==========================================
# BLOCK: 1. 固定設定
# ==========================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_KEY_FILE   = os.path.join(BASE_DIR, 'service_account.json')
SPREADSHEET_KEY = "1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I"
LOCAL_DATABASE  = "/Users/macuser/Desktop/minrepo_project/minrepo_database.csv"

# 司令官指定：母集団の純化条件
MIN_UNITS_STUDY = 5
MIN_GAMES_STUDY = 2500

def calculate_payout(diff, games):
    if games <= 0: return 100.0
    return ((games * 3 + diff) / (max(1, games) * 3)) * 100

def check_3_of_5(sorted_dts):
    """3/5日ルールの聖域：5日中3日の生存を台番号ごとに確認"""
    if len(sorted_dts) < 3: return False
    for i in range(len(sorted_dts)-2):
        if (sorted_dts[i+2] - sorted_dts[i]).days <= 4: return True
    return False

# ==========================================
# BLOCK: 2. 全軍反転解析エンジン
# ==========================================
def run_full_reversal_study():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔍 Seeker 全軍展開。全歴史から反転座標を抽出します...")
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

    # 機種別・乖離ビン別集計：reversal_stats[model][bin] = {wins, total, lift_sum}
    reversal_stats = collections.defaultdict(lambda: collections.defaultdict(lambda: {"wins": 0, "total": 0, "lift_sum": 0.0}))

    for store, models in db.items():
        all_store_dates = sorted(list(set(dt for m in models.values() for u in m.values() for dt in u.keys())))
        
        for model, units in models.items():
            # 1. 3/5ルールによる個体選別と時系列データの構築
            model_history = collections.defaultdict(lambda: {'diff': 0, 'games': 0, 'u_count': 0})
            for u_id, hist in units.items():
                if check_3_of_5(sorted(list(hist.keys()))):
                    for d, val in hist.items():
                        model_history[d]['diff'] += val['diff']
                        model_history[d]['games'] += val['games']
                        model_history[d]['u_count'] += 1
            
            if not model_history: continue

            # 2. 連続した日付リストに対してMA計算用の日次配列を作成
            daily_stats = []
            for d in all_store_dates:
                if d in model_history and model_history[d]['u_count'] > 0:
                    h = model_history[d]
                    daily_stats.append({
                        'payout': calculate_payout(h['diff'], h['games']),
                        'avg_g': h['games'] / h['u_count'],
                        'units': h['u_count']
                    })
                else:
                    daily_stats.append({'payout': 100.0, 'avg_g': 0, 'units': 0})

            # 3. 反転シミュレーション
            for i in range(30, len(daily_stats) - 3):
                # 司令官指定：5台以上 ＆ MA7G 2,500G以上の期間のみ抽出
                current_units = daily_stats[i]['units']
                ma7_g = sum(x['avg_g'] for x in daily_stats[max(0, i-6):i+1]) / 7
                
                if current_units < MIN_UNITS_STUDY or ma7_g < MIN_GAMES_STUDY:
                    continue

                # MA7とMA30の算出
                ma7_p = sum(x['payout'] for x in daily_stats[max(0, i-6):i+1]) / 7
                ma30_p = sum(x['payout'] for x in daily_stats[max(0, i-29):i+1]) / 30
                divergence = ma7_p - ma30_p
                
                # 下方乖離（逆張りチャンス）を検知
                if divergence < 0:
                    div_bin = math.floor(divergence) # -3.4% -> -4%域として集計
                    
                    # 翌日から3日間の実戦値
                    f_payouts = [daily_stats[k]['payout'] for k in range(i+1, i+4) if daily_stats[k]['units'] > 0]
                    if not f_payouts: continue
                    
                    res_p3 = sum(f_payouts) / len(f_payouts)
                    
                    # 成功判定：その日のMA30（その機種の重力）を超えたか
                    is_win = 1 if res_p3 > ma30_p else 0
                    
                    s = reversal_stats[model][div_bin]
                    s["total"] += 1
                    s["wins"] += is_win
                    s["lift_sum"] += (res_p3 - ma30_p)

    # 4. レポート行の作成
    report_rows = []
    for model, bins in reversal_stats.items():
        for div_bin, s in bins.items():
            if s["total"] < 5: continue # 信頼性担保：5件未満は除外
            win_rate = (s["wins"] / s["total"]) * 100
            avg_lift = s["lift_sum"] / s["total"]
            report_rows.append([model, f"{div_bin}%域", s["total"], f"{win_rate:.1f}%", f"{avg_lift:+.2f}%", div_bin])
    
    # 機種名、次いで乖離の深さ順にソート
    report_rows.sort(key=lambda x: (x[0], x[5]))
    return report_rows

# ==========================================
# BLOCK: 3. スプレッドシート納品
# ==========================================
def deliver_reversal_map(doc, data):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 🛠 スプレッドシートへの納品を開始します...")
    try:
        ws = doc.worksheet("Sentinel_Reversal_Study")
        ws.clear()
    except WorksheetNotFound:
        ws = doc.add_worksheet("Sentinel_Reversal_Study", 2000, 10)
    
    if not data:
        print("【警告】条件を満たすサンプルが1件も見つかりませんでした。")
        return

    header = [["機種名", "乖離の深さ(MA7-MA30)", "過去検知数", "反転成功率(対MA30)", "平均リフト幅"]]
    # API 429回避のため、1秒待機してから書き込み
    time.sleep(1)
    ws.update(values=header + [row[:5] for row in data], range_name='A1')
    print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ 黄金の反転表、納品完了。")

if __name__ == "__main__":
    creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_FILE, ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
    gc = gspread.authorize(creds); doc = gc.open_by_key(SPREADSHEET_KEY)
    map_data = run_full_reversal_study()
    deliver_reversal_map(doc, map_data)