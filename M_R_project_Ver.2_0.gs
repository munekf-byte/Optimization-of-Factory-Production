/**
 * 統合司令塔（分析設定）Version 3.2
 * 指標エリアをクロス分析内へ移動 / 行数不足エラーを完全回避
 */
function rebuildCommanderV3() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const configName = "分析設定";
  const indexName = "機種目録";
  
  let configSheet = ss.getSheetByName(configName) || ss.insertSheet(configName);
  const indexSheet = ss.getSheetByName(indexName);
  
  // 1. リストの準備
  let storeList = ["学園の森", "マルハンつくば"]; 
  let modelList = [];
  if (indexSheet) {
    const data = indexSheet.getDataRange().getValues();
    storeList = [...new Set(data.slice(1).map(r => r[0]))].filter(String);
    modelList = [...new Set(data.slice(1).map(r => r[1]))].filter(String);
  }

  // 2. 器の確保（ここがエラー回避の肝です）
  configSheet.clear();
  // 100行・10列を最低限確保する
  const currentRows = configSheet.getMaxRows();
  const currentCols = configSheet.getMaxColumns();
  if (currentRows < 100) configSheet.insertRowsAfter(currentRows, 100 - currentRows);
  if (currentCols < 10) configSheet.insertColumnsAfter(currentCols, 10 - currentCols);
  configSheet.getRange("A1:Z100").clearDataValidations();

  // 3. レイアウト構築（全行4列）
  const layout = [
    ["--- SYSTEM GLOBAL COMMAND ---", "", "", "全機能の一括実行"],
    ["全分析・一括実行", "待機中", "←「実行」で開始", "最終更新：--:--"],
    ["", "", "", ""],
    ["--- GLOBAL SETTINGS ---", "", "", "共通設定エリア"],
    ["分析対象店舗", storeList[0], "", "プルダウンで選択"],
    ["", "", "", ""],
    ["--- SINGLE MODEL ANALYSIS ---", "", "単独実行", "マトリックス分析"],
    ["ターゲット機種", "", "待機中", "←「実行」で単独更新"],
    ["--- CROSS ANALYSIS SETTINGS ---", "", "クロス実行", "3段グラフ分析"],
    ["比較指標", "差枚", "待機中", "差枚 / G数 / 機械割"],
    ["部門A (12枠名)", "部門A", "", ""]
  ];

  for (let i = 1; i <= 12; i++) layout.push([i.toString(), "", "", ""]);
  layout.push(["部門B (12枠名)", "部門B", "", ""]);
  for (let i = 1; i <= 12; i++) layout.push([i.toString(), "", "", ""]);
  layout.push(["部門C (12枠名)", "部門C", "", ""]);
  for (let i = 1; i <= 12; i++) layout.push([i.toString(), "", "", ""]);

  // 4. 書き込み
  configSheet.getRange(1, 1, layout.length, 4).setValues(layout);

  // 5. プルダウン設定
  const storeRule = SpreadsheetApp.newDataValidation().requireValueInList(storeList, true).build();
  const metricRule = SpreadsheetApp.newDataValidation().requireValueInList(["差枚", "G数", "機械割"], true).build();
  const modelRule = modelList.length > 0 ? SpreadsheetApp.newDataValidation().requireValueInList(modelList, true).build() : null;

  configSheet.getRange("B5").setDataValidation(storeRule);
  configSheet.getRange("B10").setDataValidation(metricRule); // 比較指標を10行目へ

  if (modelRule) {
    configSheet.getRange("B8").setDataValidation(modelRule); 
    configSheet.getRange("B12:B23").setDataValidation(modelRule); // 部門A
    configSheet.getRange("B25:B36").setDataValidation(modelRule); // 部門B
    configSheet.getRange("B38:B49").setDataValidation(modelRule); // 部門C
  }

  // 6. 装飾
  configSheet.getRange(1, 1, layout.length, 4).setHorizontalAlignment("center").setVerticalAlignment("middle");
  configSheet.getRange("A1:D1").setBackground("#000000").setFontColor("#ffffff").setFontWeight("bold");
  ["A4:D4", "A7:D7", "A9:D9"].forEach(r => configSheet.getRange(r).setBackground("#444444").setFontColor("#ffffff"));
  
  // 実行ボタンの配置（B2:一括, C8:単独, C10:クロス）
  ["B2", "C8", "C10"].forEach(r => {
    configSheet.getRange(r).setBackground("#ffff00").setFontWeight("bold").setBorder(true, true, true, true, true, true, "#000000", SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
  });

  configSheet.getRange("B5, B8, B10, B11, B24, B37").setBackground("#fff2cc");
  configSheet.setColumnWidth(1, 180); configSheet.setColumnWidth(2, 350); configSheet.setColumnWidth(3, 150);

  SpreadsheetApp.getUi().alert("新・司令塔（配置修正版）が完成しました！");
}

/**
 * 店舗管理マスタ（仕入れ司令塔）を構築する（修正版）
 */
function setupStoreMaster() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheetName = "店舗管理マスタ";
  let sheet = ss.getSheetByName(sheetName) || ss.insertSheet(sheetName);
  
  // 1. 掃除（命令を分割して確実に実行）
  sheet.clear();
  sheet.getRange("A1:Z100").clearDataValidations();

  // 2. ヘッダーとステータス説明の準備
  const header = [
    ["店名", "店舗タグURL", "ステータス", "収集開始日", "収集終了日", "最終収集成功日", "ステータス説明"]
  ];
  
  const descriptions = [
    ["長期収集 未着手", "新規登録用。開始〜終了日のデータを全取得"],
    ["長期収集 作業中", "Macが現在掘り進めている状態（自動遷移）"],
    ["再収集 未着手", "過去の特定期間を再度抜き直したい時"],
    ["再収集 作業中", "抜き直し作業中（自動遷移）"],
    ["巡回モード", "最新の1日分を毎日自動でチェックする"],
    ["停止", "リストに残すが、収集は一切行わない"]
  ];

  // 3. データの書き込み
  sheet.getRange(1, 1, 1, 7).setValues(header);
  
  // ステータスリスト（プルダウン用）
  const statusList = descriptions.map(d => d[0]);
  const rule = SpreadsheetApp.newDataValidation().requireValueInList(statusList, true).build();
  sheet.getRange("C2:C100").setDataValidation(rule);

  // 説明文の右側配置
  sheet.getRange(2, 7, descriptions.length, 2).setValues(descriptions);

  // 4. デザイン装飾
  sheet.getRange("A1:G1").setBackground("#000000").setFontColor("#ffffff").setFontWeight("bold").setHorizontalAlignment("center");
  sheet.getRange("G2:H7").setFontColor("#666666").setFontSize(9);
  sheet.getRange("A2:F100").setBackground("#fff2cc"); // 入力エリアを黄色に

  // 列幅の調整
  sheet.setColumnWidth(1, 150);
  sheet.setColumnWidth(2, 350);
  sheet.setColumnWidth(3, 150);
  sheet.setColumnWidth(4, 120);
  sheet.setColumnWidth(5, 120);
  sheet.setColumnWidth(6, 180);
  sheet.setColumnWidth(7, 150);
  sheet.setColumnWidth(8, 300);

  SpreadsheetApp.getUi().alert("店舗管理マスタ（Ver.1.1）が完成しました！\n店名、URL、期間、ステータスを入力してください。");
}

/**
 * 店舗管理マスタに「カレンダー選択機能」を追加する
 */
function addDatePickerToMaster() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName("店舗管理マスタ");
  
  if (!sheet) {
    SpreadsheetApp.getUi().alert("『店舗管理マスタ』シートが見つかりません。先に作成してください。");
    return;
  }

  // 日付の入力規則を作成（有効な日付であることを条件にする）
  const dateRule = SpreadsheetApp.newDataValidation()
    .requireDate()
    .setAllowInvalid(false)
    .setHelpText("カレンダーから正しい日付を選択してください。")
    .build();

  // D列（開始日）と E列（終了日）の2行目から100行目までに適用
  sheet.getRange("D2:E100").setDataValidation(dateRule);

  SpreadsheetApp.getUi().alert("D列とE列をダブルクリックするとカレンダーが出るようになりました！");
}