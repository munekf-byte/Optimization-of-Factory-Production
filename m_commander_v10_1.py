# ==========================================
# BLOCK: 1. インポート ＆ 基地基本設定
# ==========================================
# 目的: システム稼働に必要なライブラリの読み込みと、物理パスの定義。
# ------------------------------------------
import gspread
from google.oauth2.service_account import Credentials
import time
import random
import datetime
import pandas as pd
import os
import json

# --- 基地の住所設定 (PMの環境に合わせて適宜修正) ---
SPREADSHEET_ID = '1koHCi0l4KcsuMBEYSYRx_lklniibQHeCYaO_k-GUU1I'
JSON_KEY_FILE = 'service_account.json'
LOCAL_DATABASE = '/Users/macuser/Desktop/minrepo_project/minrepo_database.csv' # CSVの保管場所
SHEET_NAME_CONFIG = '分析設定'

# ==========================================
# BLOCK: 2. ハッカー・ターミナル・コンポーネント (UI)
# ==========================================
# 目的: D8セルを「生きた端末」に変貌させる視覚演出。
# ------------------------------------------
def get_hacker_noise():
    """ターミナル感を出すための16進数とシステムログの混濁"""
    hex_chars = "0123456789ABCDEF"
    fake_logs = [
        ">> ACCESSING_LOCAL_DB...", ">> DECRYPTING_CSV_STREAM...",
        ">> CACHE_MEMORY_ALLOCATED", ">> RSA_KEY_VERIFIED",
        ">> BUFFER_OVERWRITE_PROTECTION: ON", ">> ANALYZING_SLOT_PATTERN..."
    ]
    lines = []
    # 16進数ダンプ行の生成
    for _ in range(2):
        dump = " ".join(["".join(random.choice(hex_chars) for _ in range(2)) for _ in range(6)])
        lines.append(f"0x{random.randint(1000, 9999)}: {dump}")
    # 仮想ログ行の追加
    lines.append(random.choice(fake_logs))
    return "\n".join(lines)

def hacker_console(sheet, message, status="INFO"):
    """
    D8セルを更新。
    statusによってメッセージの色や雰囲気を制御（※GAS側での色制御と連動可能）
    """
    noise = get_hacker_noise()
    timestamp = datetime.datetime.now().strftime('%H:%M:%S')
    display_text = (
        f"--- [ SYSTEM_STATUS: {status} | {timestamp} ] ---\n"
        f"{noise}\n"
        f">> {message}"
    )
    try:
        sheet.update_acell('D8', display_text)
    except Exception:
        pass # 通信エラーで止まらないようガードレール
    time.sleep(0.05) # 演出用ウェイト

# ==========================================
# BLOCK: 3. インテリジェント略称生成ロジック
# ==========================================
# 目的: スマホでタブが並んだ際の視認性を極限まで高める。
# 15文字制限を意識しつつ、「シリーズの核」を死守する。
# ------------------------------------------
def get_intelligent_short_name(store_full, model_full):
    """
    例: 'KING BOSS 1000' + 'Lパチスロ革命機ヴァルヴレイヴ2'
    -> 'KING_革命機ヴ2_0207'
    """
    # 1. 店名の圧縮（最初の3文字）
    store = store_full[:3]
    
    # 2. 機種名のクリーンアップ（接頭辞の排除）
    model = model_full.replace("Lパチスロ", "").replace("Sパチスロ", "").replace("スマスロ", "").replace("パチスロ", "")
    
    # 3. シリーズ識別キーの保護（末尾にこれが含まれていたら削らずに残す）
    suffixes = ["2", "3", "4", "5", "V", "ZERO", "覚醒", "編", "Re"]
    found_suffix = ""
    for s in suffixes:
        if s in model:
            found_suffix = s
            # 本体名を少し削ってでもサフィックスを優先
            model = model.replace(s, "")
            break
    
    # 4. 本体名の抽出（4文字）
    short_model = model[:4].strip() + found_suffix
    
    # 5. 日付情報の付与
    date_tag = datetime.datetime.now().strftime('%m%d')
    
    # 6. 結合 (例: ABC_革命機ヴ2_0207)
    final_name = f"{store}_{short_model}_{date_tag}"
    
    # Google制限(31文字)を絶対に超えないよう最終カット
    return final_name[:31]


    # ==========================================
# BLOCK: 4. ワークシート・ライフサイクル管理
# ==========================================
# 目的: 「土地不足」エラーを物理的に排除する。
# 同名の古いタブをNuclear Reset（完全削除）し、広大な更地を再建築する。
# ------------------------------------------
def initialize_worksheet(ss, tab_name):
    """
    指定された名前で2000行×200列の巨大シートを新規作成する。
    既存の同名シートがある場合は、最新データ反映のために一度削除する。
    """
    try:
        # 1. 既存シートの探索と削除 (Nuclear Reset)
        existing_sheets = ss.worksheets()
        for s in existing_sheets:
            if s.title == tab_name:
                ss.del_worksheet(s)
                break
        
        # 2. 広大な土地の確保 (V8.2: Auto Grid Expansion)
        # 2000行 × 200列 = 40万セルのキャパシティを確保
        new_sheet = ss.add_worksheet(title=tab_name, rows="2000", cols="200")
        return new_sheet

    except Exception as e:
        print(f"Sheet Initialization Error: {e}")
        return None

# ==========================================
# BLOCK: 5. Dashboard（コックピット）骨組み建築
# ==========================================
# 目的: PM設計のDashboard雛形（A1:O33）をミリ単位で再現する。
# 数値が入る前の「器」を美しく整え、スマホでの視認性を確定させる。
# ------------------------------------------
def build_dashboard_skeleton(sheet, model_name, store_name):
    """
    A1:O33の範囲にDashboardの枠組みと固定ラベルを配置する。
    """
    # 1. タイトル ＆ 基本情報エリア (A1:E3)
    now_str = datetime.datetime.now().strftime('%Y/%m/%d %H:%M')
    base_info = [
        [f"■ 機種別解析報告書: {model_name}", "", "", "", f"生成日時: {now_str}"],
        [f"■ 対象店舗: {store_name}", "", "", "", "STATUS: ANALYSIS_COMPLETE"],
        ["----------------------------------------------------------------------"]
    ]
    sheet.update('A1:E3', base_info)

    # 2. メイン・スタッツ・ラベル（V9.0 Dashboard 雛形継承）
    # PMのコックピット設計に基づき、主要指標のラベルを配置
    labels = [
        ["【 総合評価 】", "", "【 突破率 】", "", "【 推定設定 】"],
        ["-", "", "-", "", "-"], # 数値用プレースホルダー
        ["", "", "", "", ""],
        ["総ゲーム数", "初当回数", "初当確率", "最大差枚", "現在差枚"],
        ["- G", "- 回", "1/ -", "- 枚", "- 枚"]
    ]
    sheet.update('A5:E9', labels)

    # 3. 三期個別サマリー用ラベル (74-80行目の倉庫直上エリア)
    # 前期・中期・後期・TOTALの成績を並べる「実戦的データ倉庫」の蓋
    summary_labels = [
        ["[ 三期個別サマリー ]"],
        ["区分", "総G数", "初当", "確率", "差枚", "突破率", "期待値"],
        ["前期", "0", "0", "1/0", "0", "0%", "±0"],
        ["中期", "0", "0", "1/0", "0", "0%", "±0"],
        ["後期", "0", "0", "1/0", "0", "0%", "±0"],
        ["TOTAL", "0", "0", "1/0", "0", "0%", "±0"]
    ]
    sheet.update('A74:G80', summary_labels)

# ==========================================
# BLOCK: 6. モバイルUI・スタイリング（列幅 ＆ 装飾）
# ==========================================
# 目的: スマホで見た時に「一画面に情報が収まる」よう列幅を極限まで調整。
# ハッカー風の「黒ベース」や「強調色」の書式設定。
# ------------------------------------------
def apply_mobile_styling(sheet):
    """
    列幅の最適化と、基本フォント・色の設定。
    """
    # 1. 列幅の精密調整 (Mobile UI 最適化)
    # A列(項目名)を少し広め、数値列をタイトに
    requests = [
        {"updateDimensionProperties": {
            "range": {"sheetId": sheet.id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1},
            "properties": {"pixelSize": 120}, "fields": "pixelSize"
        }},
        {"updateDimensionProperties": {
            "range": {"sheetId": sheet.id, "dimension": "COLUMNS", "startIndex": 1, "endIndex": 15},
            "properties": {"pixelSize": 80}, "fields": "pixelSize"
        }}
    ]
    sheet.spreadsheet.batch_update({"requests": requests})

    # 2. Dashboardエリア(A1:O33)の背景色設定
    # 視認性を高めるため、ヘッダーにグレーの背景を適用（V9.0 踏襲）
    sheet.format("A1:O1", {
        "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.2},
        "textFormat": {"foregroundColor": {"red": 0.0, "green": 1.0, "blue": 1.0}, "bold": True}
    })
    
    # 解析値入力エリア(A5:E9)を太字に
    sheet.format("A5:E9", {"textFormat": {"bold": True, "fontSize": 11}})

    # ==========================================
# BLOCK: 7. CSVデータインジェクション ＆ Pandas解析
# ==========================================
# 目的: ローカル倉庫(CSV)からデータを吸い上げ、統計的にクレンジングする。
# ------------------------------------------
def load_and_analyze_data(model_name):
    """
    指定された機種のCSVを読み込み、Pandasで基本統計量を算出する。
    """
    csv_path = os.path.join(LOCAL_DATABASE, f"{model_name}.csv")
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"DATABASE_NOT_FOUND: {model_name}")

    # データロード
    df = pd.read_csv(csv_path)
    
    # 基本的なデータクレンジング (V9.0準拠)
    # 例: 異常値の排除や、日付型の変換など
    df['G数'] = pd.to_numeric(df['G数'], errors='coerce').fillna(0)
    df['差枚'] = pd.to_numeric(df['差枚'], errors='coerce').fillna(0)
    
    # 三期分割ロジック (データを3等分して傾向を掴む)
    chunk_size = len(df) // 3
    early_df = df.iloc[:chunk_size]
    mid_df = df.iloc[chunk_size:chunk_size*2]
    late_df = df.iloc[chunk_size*2:]
    
    return {
        "total": df,
        "early": early_df,
        "mid": mid_df,
        "late": late_df,
        "count": len(df)
    }

# ==========================================
# BLOCK: 8. 期待値 ＆ 突破率演算エンジン
# ==========================================
# 目的: 抽出したデータから、PMが押し引きを判断するための「勝てる数値」を導き出す。
# ------------------------------------------
def calculate_metrics(df_set):
    """
    各期間（前・中・後・合計）の期待値・突破率・初当確率を計算。
    """
    results = []
    for key in ["early", "mid", "late", "total"]:
        df = df_set[key]
        total_g = df['G数'].sum()
        total_diff = df['差枚'].sum()
        hits = len(df[df['初当'] == 1]) # 初当フラグのカウント
        
        # 突破率計算 (例: 差枚がプラスで終わった割合)
        win_rate = (len(df[df['差枚'] > 0]) / len(df) * 100) if len(df) > 0 else 0
        
        # 初当確率
        prob = f"1/{int(total_g / hits)}" if hits > 0 else "1/0"
        
        # 期待値 (G数あたりの平均差枚から算出)
        exp_val = int(total_diff / len(df)) if len(df) > 0 else 0
        
        results.append([
            key.upper(), 
            int(total_g), 
            hits, 
            prob, 
            int(total_diff), 
            f"{win_rate:.1f}%", 
            f"{exp_val:+} 枚"
        ])
    return results

# ==========================================
# BLOCK: 9. スランプグラフ自動生成 (Google Sheets API)
# ==========================================
# 目的: 視覚的に「波」を捉えるためのグラフ描画。
# 90%〜110%スケール、ベンチマーク（グレー線）を動的に配置。
# ------------------------------------------
def generate_slump_chart(ss, sheet, data_count):
    """
    Google Sheetsの「バッチアップデート」を使い、Dashboard内にグラフを埋め込む。
    """
    sheet_id = sheet.id
    
    # グラフ追加のリクエスト作成 (V9.0: グラフ安定化ロジック)
    request = {
        "addChart": {
            "chart": {
                "spec": {
                    "title": "■ スランプグラフ・ベンチマーク分析",
                    "basicChart": {
                        "chartType": "LINE",
                        "legendPosition": "BOTTOM_LEGEND",
                        "axis": [
                            {"position": "LEFT_AXIS", "title": "累計差枚数"},
                            {"position": "BOTTOM_AXIS", "title": "試行回数"}
                        ],
                        "domains": [
                            {"domain": {"sourceRange": {"sources": [{"sheetId": sheet_id, "startRowIndex": 100, "endRowIndex": 100 + data_count, "startColumnIndex": 0, "endColumnIndex": 1}]}}}
                        ],
                        "series": [
                            {
                                "series": {"sourceRange": {"sources": [{"sheetId": sheet_id, "startRowIndex": 100, "endRowIndex": 100 + data_count, "startColumnIndex": 4, "endColumnIndex": 5}]}},
                                "targetAxis": "LEFT_AXIS",
                                "color": {"red": 0.0, "green": 1.0, "blue": 1.0} # メイン線: 水色
                            },
                            {
                                "series": {"sourceRange": {"sources": [{"sheetId": sheet_id, "startRowIndex": 100, "endRowIndex": 100 + data_count, "startColumnIndex": 10, "endColumnIndex": 11}]}},
                                "targetAxis": "LEFT_AXIS",
                                "color": {"red": 0.5, "green": 0.5, "blue": 0.5} # ベンチマーク: グレー
                            }
                        ]
                    }
                },
                "position": {
                    "overlayPosition": {
                        "anchorCell": {"sheetId": sheet_id, "rowIndex": 10, "columnIndex": 0}, # 11行目から表示
                        "offsetXPixels": 5,
                        "offsetYPixels": 5,
                        "widthPixels": 600,
                        "heightPixels": 350
                    }
                }
            }
        }
    }
    
    try:
        ss.batch_update({"requests": [request]})
    except Exception as e:
        print(f"Chart Generation Error: {e}")

# ==========================================
# BLOCK: 10. 個別機種解析・統合シーケンス
# ==========================================
# 目的: PART 1〜3 の機能を連結し、1機種分の解析を完遂させる。
# ------------------------------------------
def run_single_unit_analysis(model_name, ss, config_sheet, slot_num):
    """
    1台の機種に対し、建築・解析・描画の全工程をワンストップで実行する。
    """
    try:
        # 1. 命名 (PART 1: BLOCK 3)
        store_name = "STORE_A" # 必要に応じてCSVヘッダー等から動的取得
        short_tab_name = get_intelligent_short_name(store_name, model_name)
        
        hacker_console(config_sheet, f"TARGET_ACQUIRED: {model_name}\n>> DEPLOYING_TAB: {short_tab_name}", "BUSY")

        # 2. 土地確保 ＆ 建築 (PART 2: BLOCK 4-5)
        new_sheet = initialize_worksheet(ss, short_tab_name)
        if not new_sheet: return False
        
        build_dashboard_skeleton(new_sheet, model_name, store_name)
        apply_mobile_styling(new_sheet)

        # 3. データ解析 (PART 3: BLOCK 7-8)
        data_set = load_and_analyze_data(model_name)
        metrics = calculate_metrics(data_set)

        # 4. 数値の流し込み (V9.0 マッピング継承)
        # Dashboardメイン (A6:E6)
        total_data = metrics[3] # TOTAL行
        main_stats = [[total_data[1], total_data[2], total_data[3], total_data[4], total_data[4]]] # 仮
        new_sheet.update('A6:E6', main_stats)

        # 三期個別サマリー (A76:G79)
        new_sheet.update('A76:G79', metrics)

        # 5. グラフ生成 (PART 3: BLOCK 9)
        generate_slump_chart(ss, new_sheet, data_set["count"])

        hacker_console(config_sheet, f"SLOT {slot_num} COMPLETED.\n>> {short_tab_name}: ONLINE.", "SUCCESS")
        return True

    except Exception as e:
        hacker_console(config_sheet, f"SLOT {slot_num} FAILURE: {str(e)}", "ERROR")
        return False

# ==========================================
# BLOCK: 11. 5連射メインループ ＆ 監視エンジン
# ==========================================
# 目的: 司令塔(C8)を監視し、5つのスロットを連続爆撃する。
# ------------------------------------------
def start_commander_v10():
    # Google認証 ＆ 接続
    creds = Credentials.from_service_account_file(JSON_KEY_FILE, 
        scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'])
    gc = gspread.authorize(creds)
    ss = gc.open_by_key(SPREADSHEET_ID)
    config_sheet = ss.worksheet(SHEET_NAME_CONFIG)

    print(f"--- V10.0 SYSTEM_AWAKENING: ONLINE ---")
    print(f"--- MONITORING START: {SHEET_NAME_CONFIG} ---")

    while True:
        try:
            # 1. 起動トリガー監視 (C8セル)
            trigger = config_sheet.acell('C8').value
            
            if trigger == 'GO':
                # 2. 起動シーケンス開始
                config_sheet.update_acell('C8', 'BUSY...')
                hacker_console(config_sheet, ">> MULTI_SCAN_INITIATED\n>> AUTHORIZING_ACCESS...", "BUSY")

                # 3. スロットスキャン (B9:B13)
                # クロス分析エリア(15行目以降)を汚さず、B9:B13のみを狙い撃ち
                slot_data = config_sheet.get('B9:B13')
                targets = [row[0] for row in slot_data if row and row[0]]

                if not targets:
                    hacker_console(config_sheet, ">> ABORT: NO_TARGETS_IN_SLOTS", "ERROR")
                    config_sheet.update_acell('C8', 'GO')
                    continue

                # 4. 連続解析実行
                for i, model_name in enumerate(targets):
                    run_single_unit_analysis(model_name, ss, config_sheet, i+1)

                # 5. 全ミッション完了
                now = datetime.datetime.now().strftime('%H:%M:%S')
                hacker_console(config_sheet, f">>> ALL_UNITS_DEPLOYED.\n>>> COMPLETED_AT: {now}\n>>> SYSTEM_READY.", "SUCCESS")
                
                # ステータスを「COMPLETE」へ（GAS側のタイマーで黄色に戻るのを待機）
                config_sheet.update_acell('C8', 'COMPLETE')

            # 監視間隔 (GoogleのAPI制限を考慮し10秒)
            time.sleep(10)

        except Exception as e:
            print(f"SYSTEM_ERROR: {e}")
            time.sleep(20) # エラー時は少し間隔を空けて復帰試行

# ==========================================
# BLOCK: 12. 最終エントリーポイント
# ==========================================
if __name__ == "__main__":
    start_commander_v10()