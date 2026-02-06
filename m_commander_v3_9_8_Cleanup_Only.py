# --- VERSION: m_commander_v3_9_8_Cleanup_Only_20260205 ---
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import asyncio
import csv
import collections

SPREADSHEET_KEY = "1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I"
CONFIG_SHEET    = "分析設定"
SINGLE_SHEET    = "機種別分析"
LOCAL_DATABASE  = "/Users/macuser/Desktop/minrepo_project/minrepo_database.csv"

def hex_to_rgb(hex_str):
    hex_str = hex_str.lstrip('#')
    return {"red": int(hex_str[0:2], 16)/255.0, "green": int(hex_str[2:4], 16)/255.0, "blue": int(hex_str[4:6], 16)/255.0}

async def execute_single_analysis(doc, conf):
    print(f"   > ゴミ掃除開始: {conf['target_model']}...")
    model_data = collections.defaultdict(dict)
    unique_units = set()
    processed_keys = set()
    
    # --- 第1・2層：読み込み時の厳格な検品 ---
    with open(LOCAL_DATABASE, mode='r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            if len(row) < 6: continue
            d_date, d_store, d_model, d_unit, d_diff, d_games = [c.strip() for c in row]
            
            # 1. 基本的な店舗・機種一致
            if conf['store'] not in d_store or conf['target_model'] not in d_model: continue
            
            # 2. 【ゴミ排除】漢字が入っていたら無視（平均、合計、計 など）
            if any(k in d_unit for k in ["平", "均", "計", "合"]): continue
            
            # 3. 【ゴミ排除】数値範囲の制限（1番台〜3000番台程度が現実的。4506等はここで弾く）
            if not d_unit.isdigit(): continue
            u_int = int(d_unit)
            if u_int <= 0 or u_int > 4000: continue # 4000番以上の台は集計値とみなす
            
            # 4. 重複排除
            p_key = (d_date, u_int)
            if p_key in processed_keys: continue
            processed_keys.add(p_key)

            unique_units.add(u_int)
            model_data[d_date][u_int] = {'diff': int(d_diff), 'games': int(d_games)}

    if not unique_units: 
        print("   ! 該当する正常なデータが見つかりませんでした。")
        return
        
    sorted_units = sorted(list(unique_units))
    sorted_dates = sorted(model_data.keys())
    dow_names = ["月", "火", "水", "木", "金", "土", "日"]
    
    # --- サマリー計算 (H列(index 7)以降に限定) ---
    # summary_xxx[0:7]までは「ラベル」と「空欄」で固定し、絶対にC:G列を汚さない
    summary_10k = ["10,000枚突破率", ""] + [""] * 5
    summary_5k  = ["5,000枚突破率", ""] + [""] * 5
    summary_avg = ["台別平均差枚", ""] + [""] * 5
    
    for u in sorted_units:
        diffs = [model_data[d][u]['diff'] for d in sorted_dates if u in model_data[d]]
        total = len(diffs)
        summary_10k.append(f"{round(len([v for v in diffs if v>=10000])/total*100, 1)}%" if total > 0 else "0%")
        summary_5k.append(f"{round(len([v for v in diffs if v>=5000])/total*100, 1)}%" if total > 0 else "0%")
        summary_avg.append(int(sum(diffs)/total) if total > 0 else 0)

    header = ["日付", "曜日", "総計", "台平均", "平均G", "機械割", "粘り勝率"] + [f"{u}番台" for u in sorted_units]
    
    # --- 日次データ ---
    data_rows = []
    for d_str in sorted_dates:
        day = model_data[d_str]
        t_d, t_g = sum(u['diff'] for u in day.values()), sum(u['games'] for u in day.values())
        u_cnt = len(day)
        avg_d, avg_g = t_d/u_cnt, t_g/u_cnt
        m_rate = ((t_g * 3 + t_d) / (t_g * 3)) * 100 if t_g > 0 else 0
        sticky = len([u for u in day.values() if u['games']>=5000 and u['diff']>0])/u_cnt
        row = [d_str, dow_names[datetime.strptime(d_str, "%Y/%m/%d").weekday()], t_d, int(avg_d), int(avg_g), m_rate, sticky]
        for u in sorted_units:
            row.append(day[u]['diff'] if u in day else "")
        data_rows.append(row)

    ws = doc.worksheet(SINGLE_SHEET); ws.clear()
    # 書き込み (A21から)
    ws.update(values=[summary_10k, summary_5k, summary_avg, header], range_name='A21')
    ws.update(values=data_rows, range_name='A25')
    
    # --- 書式の適用 ---
    s_id = ws.id; l_row = len(data_rows) + 25; l_col = len(header)
    reqs = [
        # A列(100), B列(40), C-G列(85)
        {"updateDimensionProperties": {"range": {"sheetId": s_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1}, "properties": {"pixelSize": 100}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": s_id, "dimension": "COLUMNS", "startIndex": 1, "endIndex": 2}, "properties": {"pixelSize": 40}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": s_id, "dimension": "COLUMNS", "startIndex": 2, "endIndex": 7}, "properties": {"pixelSize": 85}, "fields": "pixelSize"}},
        # サマリー背景
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 20, "endRowIndex": 23}, "cell": {"userEnteredFormat": {"backgroundColor": hex_to_rgb("#f3f3f3"), "textFormat": {"bold": True}, "horizontalAlignment": "CENTER"}}, "fields": "userEnteredFormat"}},
        # 集計エリア(C-G)の表示形式
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 24, "endRowIndex": l_row, "startColumnIndex": 2, "endColumnIndex": 4}, "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0\"枚\""}}}, "fields": "userEnteredFormat"}},
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 24, "endRowIndex": l_row, "startColumnIndex": 4, "endColumnIndex": 5}, "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0\"G\""}}}, "fields": "userEnteredFormat"}},
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 24, "endRowIndex": l_row, "startColumnIndex": 5, "endColumnIndex": 6}, "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "0.00\"%\""}}}, "fields": "userEnteredFormat"}},
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 24, "endRowIndex": l_row, "startColumnIndex": 6, "endColumnIndex": 7}, "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}}, "fields": "userEnteredFormat"}},
        # データ全体のセンタリング
        {"repeatCell": {"range": {"sheetId": s_id, "startRowIndex": 24, "endRowIndex": l_row, "startColumnIndex": 0, "endColumnIndex": l_col}, "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}}, "fields": "userEnteredFormat"}},
    ]
    # 空欄グレー
    t_range = {"sheetId": s_id, "startRowIndex": 21, "endRowIndex": l_row, "startColumnIndex": 7, "endColumnIndex": l_col}
    reqs.append({"addConditionalFormatRule": {"rule": {"ranges": [t_range], "booleanRule": {"condition": {"type": "BLANK"}, "format": {"backgroundColor": hex_to_rgb("#efefef")}}}, "index": 0}})
    
    doc.batch_update({"requests": reqs})
    print("   -> ゴミ排除完了。美しくなりました。")

async def main():
    print(f"\n--- Ver.3.9.8 起動 (Trash Collector Edition) ---")
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
    gc = gspread.authorize(creds); doc = gc.open_by_key(SPREADSHEET_KEY)
    while True:
        try:
            conf_ws = doc.worksheet(CONFIG_SHEET); vals = conf_ws.get_all_values()
            single_cmd = vals[7][2] # C8セル
            if "実行" in str(single_cmd):
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 指令受信。")
                conf = {"store": vals[4][1], "target_model": vals[7][1]}
                conf_ws.update_acell('C8', "● 実行中")
                await execute_single_analysis(doc, conf)
                conf_ws.update_acell('C8', "待機中")
            conf_ws.update_acell('D2', f"監視中: {datetime.now().strftime('%H:%M:%S')}")
        except Exception as e: print(f"Error: {e}")
        await asyncio.sleep(15)

if __name__ == "__main__": asyncio.run(main())