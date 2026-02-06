# --- VERSION: migrate_to_csv.py_v1.0_20260130 ---

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import csv
import os

# ==========================================
# BLOCK: 1. 設定エリア
# ==========================================
SPREADSHEET_KEY = "1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I"
RAW_DATA_SHEET  = "生データ"
# Mac内に保存するファイル名
LOCAL_FILE      = "minrepo_database.csv"

# ==========================================
# BLOCK: 2. 引越し実行エンジン
# ==========================================
def run_migration():
    print("\n--- [ データの引越し（スプレッドシート → CSV）を開始 ] ---")
    
    # Google Sheets 認証
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    gc = gspread.authorize(creds)
    
    try:
        doc = gc.open_by_key(SPREADSHEET_KEY)
        raw_ws = doc.worksheet(RAW_DATA_SHEET)
        
        print(f"1. スプレッドシートから '{RAW_DATA_SHEET}' を読み込んでいます...")
        print("(27万行ある場合、2〜3分かかることがありますが、じっと待ってください)")
        
        # 全データを取得
        all_data = raw_ws.get_all_values()
        
        print(f"2. 読み込み完了（合計: {len(all_data)} 行）")
        
        # CSVファイルに書き出す
        print(f"3. Macのローカルファイル '{LOCAL_FILE}' へ書き込んでいます...")
        with open(LOCAL_FILE, mode='w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(all_data)
            
        print(f"\n【成功】引越しが完了しました！")
        print(f"これからは、Mac内の '{LOCAL_FILE}' を使って超高速分析が可能になります。")

    except Exception as e:
        print(f"【エラー】引越し中にトラブルが発生しました: {e}")

if __name__ == "__main__":
    run_migration()