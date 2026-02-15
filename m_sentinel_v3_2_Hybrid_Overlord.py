# --- VERSION: m_sentinel_v3_2_Hybrid_Overlord ---
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import asyncio
import csv
import collections
import os
import time
import requests
import hashlib

# ==========================================
# BLOCK: 1. 固定設定
# ==========================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_KEY_FILE   = os.path.join(BASE_DIR, 'service_account.json')
SPREADSHEET_KEY = "1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I"
LOCAL_DATABASE  = "/Users/macuser/Desktop/minrepo_project/minrepo_database.csv"

# Discord Webhook
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1471366357621805108/W9ab5EyTFQeuG1z3TGeoldwJL7k-2BPR-YHcJ_0QpkibUj9hVFoH547-Z3O1E5B_hnWx" 
DISCORD_VALIDATION_WEBHOOK_URL = "https://discord.com/api/webhooks/1471366574438092972/3TVbePfZYzsGbafE8IU09Ucoipc5VMw8xQCHXJImYKxMVb8cwu28lx6czEGbq6phwsze" 

SCAN_INTERVAL_SEC = 3600 

def calculate_payout(diff, games):
    if games <= 0: return 100.0
    return ((games * 3 + diff) / (max(1, games) * 3)) * 100

def check_3_of_5(sorted_dts):
    if len(sorted_dts) < 3: return False
    for i in range(len(sorted_dts)-2):
        if (sorted_dts[i+2] - sorted_dts[i]).days <= 4: return True
    return False

# ==========================================
# BLOCK: 2. 戦術知能の同期 (Veterans Sync)
# ==========================================
def load_veteran_brain(doc):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 🧠 熟練機名簿を同期中...")
    try:
        ws = doc.worksheet("Sentinel_Veteran_Tactics")
        records = ws.get_all_records()
        brain = {}
        for r in records:
            # (店舗名, 機種名) をキーにする
            key = (str(r['店舗名']).strip(), str(r['機種名']).strip())
            # 反転臨界点の数値を抽出（例: "-4%" -> -4.0）
            raw_point = str(r.get('反転臨界点', 'N/A'))
            target_point = float(raw_point.replace('%','')) if '%' in raw_point else None
            
            brain[key] = {
                "is_veteran": (r.get('熟練判定') == '熟練'),
                "type": r.get('区分', '不明'),
                "target_point": target_point,
                "reversal_wr": r.get('反転成功率', 'N/A'),
                "trials": r.get('試行回数', 0)
            }
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 同期完了: {len(brain)} 機種の戦術データをロード。")
        return brain
    except Exception as e:
        print(f"【警告】戦術脳の同期失敗: {e}")
        return {}

# ==========================================
# BLOCK: 3. ハイブリッド哨戒エンジン
# ==========================================
async def run_hybrid_scan(veteran_brain, doc):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] ⚡️ 精密哨戒中（ハイブリッド・モード）...")
    if not os.path.exists(LOCAL_DATABASE): return []

    # データロード
    db = collections.defaultdict(lambda: collections.defaultdict(lambda: collections.defaultdict(dict)))
    with open(LOCAL_DATABASE, mode='r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 6: continue
            try:
                d_date, d_store, d_model, d_unit, d_diff, d_games = [c.strip() for c in row]
                db[d_store][d_model][int(d_unit)][d_date] = {'diff': int(d_diff), 'games': int(d_games)}
            except: continue
    
    # 【検証報告処理の呼び出し】 (v2.8.1の機能を継承)
    # ※ここでは簡略化のため内部定義せず、以前の run_validation_process と同等のロジックを想定
    
    found_alerts = []
    for store, models in db.items():
        all_dates = sorted(list(set(d for m in models.values() for u in m.values() for d in u.keys())))
        if not all_dates: continue
        latest_date = all_dates[-1]
        dt_latest = datetime.strptime(latest_date, "%Y/%m/%d")
        
        # 6ヶ月Alpha用ベースライン
        six_months_ago = (dt_latest - timedelta(days=180)).strftime("%Y/%m/%d")
        st_6m_payouts = []
        for d in all_dates:
            if d >= six_months_ago:
                d_diff = sum(u_d[d]['diff'] for m in models.values() for u_d in m.values() if d in u_d)
                d_games = sum(u_d[d]['games'] for m in models.values() for u_d in m.values() if d in u_d)
                if d_games > 0: st_6m_payouts.append(calculate_payout(d_diff, d_games))
        store_6m_avg = sum(st_6m_payouts)/len(st_6m_payouts) if st_6m_payouts else 100.0

        for model, units in models.items():
            if not any(latest_date in u_hist for u_hist in units.values()): continue
            unit_count = len([u_id for u_id, hist in units.items() if latest_date in hist])
            
            # 生存台集計
            model_history = collections.defaultdict(lambda: {'diff': 0, 'games': 0})
            for u_id, hist in units.items():
                if check_3_of_5(sorted([datetime.strptime(d, "%Y/%m/%d") for d in hist.keys()])):
                    for d, val in hist.items():
                        model_history[d]['diff'] += val['diff']; model_history[d]['games'] += val['games']
            
            if not model_history: continue
            p_dates = sorted(model_history.keys())
            payouts = [calculate_payout(model_history[d]['diff'], model_history[d]['games']) for d in p_dates]
            if len(payouts) < 31: continue
            
            # 現在値の算出
            ma7_now, ma30_now = sum(payouts[-7:])/7, sum(payouts[-30:])/30
            ma7_pre, ma30_pre = sum(payouts[-8:-1])/7, sum(payouts[-31:-1])/30
            current_divergence = ma7_now - ma30_now
            
            # 6ヶ月Alpha
            m_6m_p = [calculate_payout(model_history[d]['diff'], model_history[d]['games']) for d in p_dates if d >= six_months_ago]
            alpha = (sum(m_6m_p)/len(m_6m_p) - store_6m_avg) if m_6m_p else 0.0

            # 戦術データ照合
            vt = veteran_brain.get((store.strip(), model.strip()), {"is_veteran": False, "target_point": None})
            
            alert_type = None
            # 1. GC（順張り）検知
            if ma7_now > ma30_now and ma7_pre <= ma30_pre:
                alert_type = "GC"
            # 2. 反転臨界点（逆張り）検知
            elif vt["is_veteran"] and vt["target_point"] is not None:
                if current_divergence <= vt["target_point"]:
                    # 反転シグナルは「今日初めて臨界点に達した」場合のみ送る（簡易的に昨日の乖離と比較）
                    div_pre = ma7_pre - ma30_pre
                    if div_pre > vt["target_point"]:
                        alert_type = "REVERSAL"

            if alert_type:
                tg_id = f"TG-{latest_date.replace('/','')}-{hashlib.md5((store+model+alert_type).encode()).hexdigest()[:4].upper()}"
                found_alerts.append({
                    "type": alert_type, "store": store, "model": model, "date": latest_date,
                    "unit_count": unit_count, "alpha": round(alpha, 1),
                    "ma7": round(ma7_now, 1), "ma30": round(ma30_now, 1),
                    "div": round(current_divergence, 2),
                    "tg_id": tg_id, "tactical": vt
                })
    return found_alerts

# ==========================================
# BLOCK: 4. 報告処理（ハイブリッド通知フォーマット）
# ==========================================
def send_hybrid_alert(alerts):
    for a in alerts:
        t = a['tactical']
        v_tag = "【熟練】" if t['is_veteran'] else ""
        
        if a['type'] == "GC":
            emoji = "🔴" if t.get('is_veteran') else "⚪"
            label = f"{v_tag}特級シグナル" if t.get('is_veteran') else "通常シグナル"
            header = f"{emoji} **GC_{label}** {emoji}"
            body = (
                f"格付：`{t.get('type', '新規')}` / 信頼：`{t.get('trials', 0)}回`"
            )
        else: # REVERSAL
            emoji = "🔵"
            header = f"{emoji} **VETERAN_【熟練・反転シグナル】** {emoji}"
            body = (
                f"臨界点：`{t['target_point']}%` 到達\n"
                f"反転率：`{t['reversal_wr']}` (過去{t['trials']}回)"
            )

        msg = (
            f"{header}\n\n"
            f"店舗：**{a['store']}**\n\n"
            f"機種：**{a['model']}**\n\n"
            f"--- 戦術データ ---\n"
            f"発生：{a['date']}\n"
            f"設置台数：{a['unit_count']}台\n"
            f"{body}\n"
            f"現在値：MA7({a['ma7']}%) / MA30({a['ma30']}%)\n"
            f"乖離度：Alpha({a['alpha']}%)\n\n"
            f"ID：`{a['tg_id']}`\n"
            f"--- --- ---"
        )
        # 設置台数フィルタ（GCは3台以下スキップ。反転はそもそも熟練機=4台以上のみ）
        if a['unit_count'] > 3:
            requests.post(DISCORD_WEBHOOK_URL, json={"content": msg})
            time.sleep(1.0)

# ==========================================
# BLOCK: 5. メインループ
# ==========================================
async def main():
    print(f"--- Sentinel Hybrid Overlord v3.2 起動 ---")
    creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_FILE, ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
    gc = gspread.authorize(creds); doc = gc.open_by_key(SPREADSHEET_KEY)
    
    while True:
        try:
            # 1. 熟練機知能の同期
            veteran_brain = load_veteran_brain(doc)
            # 2. ハイブリッド哨戒
            alerts = await run_hybrid_scan(veteran_brain, doc)
            # 3. 通知
            send_hybrid_alert(alerts)
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 哨戒周期完了。待機。")
            await asyncio.sleep(SCAN_INTERVAL_SEC)
        except Exception as e:
            print(f"ERROR: {e}"); await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())