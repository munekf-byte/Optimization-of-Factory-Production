/**
 * ヴァルヴレイヴ2 マスター管理システム
 * Ver 4.0 (Full Reboot & Bonus Fix - Complete)
 * 更新日: 2026/01/24
 */

// ====================================
// グローバル設定
// ====================================
const CONFIG = {
  MASTER_SHEET_ID: XXXXXXXXXXXXXXXX,
  NORMAL_RANGE: 'XXXXXXXXXXXXXXXX',
  AT_RANGE: 'XXXXXXXXXXXXXXXX',
  MIN_BLOCKS: 5, 
  BLOCK_SIZE: 3, 
  AT_BLOCK_SIZE: 2,
  NORMAL_COLS: 42,
  AT_COLS: 20,
  TIME_LIMIT: 5 * 60 * 1000
};

// ====================================
// データ収集メイン処理
// ====================================
function collectAllUserData() {
  const startTime = new Date().getTime();
  console.log('===== データ収集開始 (Ver 4.0) =====');
  
  const props = PropertiesService.getScriptProperties();
  const lastIndex = parseInt(props.getProperty('LAST_PROCESSED_INDEX') || '0');
  
  const master = SpreadsheetApp.getActiveSpreadsheet();
  const userSheet = master.getSheetByName('ユーザー管理');
  const collectionHistorySheet = master.getSheetByName('収集履歴管理');
  const normalDataSheet = master.getSheetByName('通常時データ集約');
  const atDataSheet = master.getSheetByName('ATデータ集約');
  
  if (!collectionHistorySheet || !normalDataSheet || !atDataSheet) {
    setupAllHeaders();
  }

  let summarySheet = master.getSheetByName('収支管理');
  if (!summarySheet) {
    summarySheet = master.insertSheet('収支管理');
    setupAllHeaders();
  }
  
  const errorLogSheet = master.getSheetByName('エラーログ');
  const users = userSheet.getDataRange().getValues().slice(1);
  console.log(`全ユーザー数: ${users.length} (開始インデックス: ${lastIndex})`);
  
  if (lastIndex === 0 && summarySheet.getLastRow() > 1) {
    console.log('収支サマリークリア...');
    summarySheet.getRange(2, 1, summarySheet.getLastRow() - 1, summarySheet.getLastColumn()).clearContent();
  }
  
  let isTimeOut = false;
  
  for (let i = lastIndex; i < users.length; i++) {
    if (new Date().getTime() - startTime > CONFIG.TIME_LIMIT) {
      console.log(`⏳ タイムアウト接近。${i}件目で中断します。`);
      props.setProperty('LAST_PROCESSED_INDEX', i.toString());
      setResumeTrigger();
      isTimeOut = true;
      break;
    }
    
    const user = users[i];
    const [userId, name, email, sheetUrl, , , status] = user;
    
    if (!sheetUrl || !userId) continue;
    if (status !== 'アクティブ') continue;
    
    try {
      console.log(`処理中 (${i+1}/${users.length}): ${name}`);
      
      const sheetIdMatch = sheetUrl.match(/\/d\/([a-zA-Z0-9-_]+)/);
      if (!sheetIdMatch) throw new Error('不正なシートURL');
      
      const userSpreadsheet = SpreadsheetApp.openById(sheetIdMatch[1]);
      const sheets = userSpreadsheet.getSheets();
      
      sheets.forEach(sheet => {
        const sheetName = sheet.getName();
        const sheetId = sheet.getSheetId();
        const excludeSheets = ['集計', '革命の軌跡', '_集計データ', '_system'];
        if (excludeSheets.includes(sheetName) || sheetName.startsWith('_')) return;
        
        const summaryData = collectSummaryData(sheet, userId, sheetId, sheetName);
        appendToSheet(summarySheet, [summaryData]);
        
        const shouldCollect = checkShouldCollect(collectionHistorySheet, userId, sheetId, sheetName);
        if (!shouldCollect.collect) return;
        
        const normalData = collectNormalData(sheet, userId, sheetId, sheetName);
        if (normalData.length > 0) appendToSheet(normalDataSheet, normalData);
        
        const atData = collectATData(sheet, userId, sheetId, sheetName);
        if (atData.length > 0) appendToSheet(atDataSheet, atData);
        
        updateCollectionHistory(collectionHistorySheet, userId, sheetId, sheetName, normalData.length, atData.length);
        console.log(`  ✅ 収集: ${sheetName}`);
      });
      
      userSheet.getRange(i + 2, 6).setValue(new Date());
      
    } catch (error) {
      if (errorLogSheet) errorLogSheet.appendRow([new Date(), userId, '', error.toString(), '']);
      console.log(`  ❌ エラー: ${error.toString()}`);
    }
    
    Utilities.sleep(500);
  }
  
  if (!isTimeOut) {
    console.log('===== 全件完了 =====');
    props.deleteProperty('LAST_PROCESSED_INDEX');
    deleteResumeTrigger();
    console.log('ユーザーセグメント更新...');
    updateUserSegments(master);
    console.log('モード再計算開始...');
    recalculateMasterModes();
    console.log('===== 全工程完了 =====');
  }
}

// ====================================
// 収支サマリー収集 (Fix: Event2 Check)
// ====================================
function collectSummaryData(sheet, userId, sheetId, sheetName) {
  try {
    const collectTime = new Date();
    const diff = sheet.getRange('CO115').getValue();
    const inv = sheet.getRange('CO92').getValue();
    const exc = sheet.getRange('AC81').getValue();
    const bal = sheet.getRange('AE81').getValue();
    const myDiff = (sheet.getRange('V81').getValue() || 0);
    const totalG = sheet.getRange('CN82').getValue();
    const normalG = sheet.getRange('CN84').getValue();
    
    const nRange = sheet.getRange(CONFIG.NORMAL_RANGE).getValues();
    const ev1 = nRange.map(r => (r[4]||'').toString());
    const ev2 = nRange.map(r => (r[5]||'').toString());
    const atCol = nRange.map(r => (r[9]||'').toString());
    
    let czC=0, czS=0, kakuC=0, kakuS=0, kessC=0, kessS=0;
    
    for(let r=0; r<nRange.length; r++) {
      if(!nRange[r][0] && !nRange[r][4] && !nRange[r][9]) continue;
      
      const e1 = ev1[r], e2 = ev2[r], at = atCol[r];
      const isCZ = (e1 === 'CZ');
      const isAT = (at && at.includes('AT'));
      const isSuccess = (e2.match(/革命|決戦|RUSH/));
      
      const isKaku = (e1.includes('革命BONUS') || e2.includes('革命BONUS'));
      const isKess = (e1.includes('決戦BONUS') || e2.includes('決戦BONUS'));
      
      if(isCZ) { czC++; if(isSuccess) czS++; }
      if(isKaku) { kakuC++; if(isAT) kakuS++; }
      if(isKess) { kessC++; if(isAT) kessS++; }
    }
    
    const ningen = sheet.getRange('DG108').getValue();
    const ld = sheet.getRange('DG103').getValue();
    const ldh = sheet.getRange('DG106').getValue();
    const denno = sheet.getRange('CY97').getValue();
    const direct = sheet.getRange('CY84').getValue();
    const ldSuccess = sheet.getRange('DG105').getValue();
    
    return [
      collectTime, userId, sheetId, sheetName, diff, inv, exc, bal, myDiff, totalG, normalG, 
      czC, czS, kakuC, kakuS, kessC, kessS, 
      ningen, ld, ldh, denno, direct, ldSuccess
    ];
  } catch (error) {
    console.log(`❌ 収支収集エラー: ${sheetName} - ${error}`);
    return [new Date(), userId, sheetId, sheetName, ...new Array(19).fill(0)];
  }
}

// ====================================
// ヘッダー再構築 (全シート対応)
// ====================================
function setupAllHeaders() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  
  let normSheet = ss.getSheetByName('通常時データ集約');
  if (!normSheet) normSheet = ss.insertSheet('通常時データ集約');
  const normHeaders = ['収集日時','ユーザーID','シートID','シート名','行番号','稼働内連番','実G','規定pt履歴','周期','契機','イベント1','イベント2','示唆','革ボ情報','革ボ獲得','AT初当り','AT実績','AT獲得枚数','1-1','1-2','1-3','2-1','2-2','2-3','3-1','3-2','3-3','4-1','4-2','4-3','5-1','5-2','5-3','自動反映メモ','手書き記入メモ','特殊モード推測結果','通常時予備枠2','モード推測結果','確定設定情報','予備2','予備3','予備4','予備5','予備6','予備7','予備8','予備9','予備10','総実G数','ボイス記録数','ボイスメモ信頼性','CZ発生回数','CZ失敗記録数','CZ記録信頼性','モード推測使用率','枚数スタンプ使用率','データ精度総合','特殊推測(再計算)','モード推測(再計算)'];
  normSheet.getRange(1, 1, 1, normHeaders.length).setValues([normHeaders]).setFontWeight('bold').setBackground('#4a86e8').setFontColor('white');
  
  let atSheet = ss.getSheetByName('ATデータ集約');
  if (!atSheet) atSheet = ss.insertSheet('ATデータ集約');
  const atHeaders = ['収集日時','ユーザーID','シートID','シート名','行番号','稼働内AT連番','AT番号','R種別','継続契機','状態','枚数記録','切断','道中乗せ','R画面','革命の剣','革命の剣','特殊枚数','R画面','特殊演出','フリーメモ／備考','枚数スタンプ','確定情報','AT予備2','AT予備3','AT予備4','AT予備5','AT中差枚数スタンプ'];
  atSheet.getRange(1, 1, 1, atHeaders.length).setValues([atHeaders]).setFontWeight('bold').setBackground('#e69138').setFontColor('white');
  
  let histSheet = ss.getSheetByName('収集履歴管理');
  if (!histSheet) histSheet = ss.insertSheet('収集履歴管理');
  const histHeaders = ['ユーザーID','シートID','シート名','初回収集日時','最終収集日時','データ更新日時','通常行数','AT行数','ステータス','備考'];
  histSheet.getRange(1, 1, 1, histHeaders.length).setValues([histHeaders]).setFontWeight('bold').setBackground('#666666').setFontColor('white');

  let sumSheet = ss.getSheetByName('収支管理');
  if (!sumSheet) sumSheet = ss.insertSheet('収支管理');
  const sumHeaders = ['収集日時','ユーザーID','シートID','シート名','計算上差枚','実投資枚数','交換枚数','最終収支','理論差枚','総G数','通常G数','CZ回数','CZ成功','革ボ回数','革ボ成功','決戦回数','決戦成功','ニンゲン','LD','LDH','電脳回数','直撃回数','LD成功数'];
  sumSheet.getRange(1, 1, 1, sumHeaders.length).setValues([sumHeaders]).setFontWeight('bold').setBackground('#20124d').setFontColor('white');
  
  Logger.log('全ヘッダー復旧完了');
}

// ====================================
// 補助関数群 (復活)
// ====================================
function appendToSheet(sheet, data) {
  if (data.length === 0) return;
  const startRow = sheet.getLastRow() + 1;
  sheet.getRange(startRow, 1, data.length, data[0].length).setValues(data);
}

function updateCollectionHistory(historySheet, userId, sheetId, sheetName, normalRows, atRows) {
  const historyData = historySheet.getDataRange().getValues();
  const now = new Date();
  const existingIndex = historyData.slice(1).findIndex(row => row[0] === userId && row[1] === sheetId);
  if (existingIndex === -1) {
    historySheet.appendRow([userId, sheetId, sheetName, now, now, now, normalRows, atRows, '収集済', '']);
  } else {
    const rowNum = existingIndex + 2;
    historySheet.getRange(rowNum, 5).setValue(now);
    historySheet.getRange(rowNum, 6).setValue(now);
    historySheet.getRange(rowNum, 7).setValue(normalRows);
    historySheet.getRange(rowNum, 8).setValue(atRows);
  }
}

function checkShouldCollect(collectionHistorySheet, userId, sheetId, sheetName) {
  const historyData = collectionHistorySheet.getDataRange().getValues();
  const today = new Date().setHours(0, 0, 0, 0);
  const existingRecord = historyData.slice(1).find(row => row[0] === userId && row[1] === sheetId);
  if (!existingRecord) return { collect: true, reason: '新規シート' };
  const lastCollectedDate = new Date(existingRecord[4]).setHours(0, 0, 0, 0);
  if (lastCollectedDate < today) return { collect: false, reason: '既収集（日跨ぎ）' };
  return { collect: true, reason: '同日内更新' };
}

function padArray(arr, len) {
  if (!arr) return new Array(len).fill('');
  const newArr = [...arr];
  while (newArr.length < len) newArr.push('');
  return newArr.slice(0, len);
}

function setResumeTrigger() {
  const triggers = ScriptApp.getProjectTriggers();
  const handler = 'collectAllUserData';
  if (triggers.filter(t => t.getHandlerFunction() === handler).length < 2) {
    ScriptApp.newTrigger(handler).timeBased().after(60 * 1000).create();
  }
}

function deleteResumeTrigger() {
  const triggers = ScriptApp.getProjectTriggers();
  triggers.forEach(t => {
    if (t.getHandlerFunction() === 'collectAllUserData' && t.getTriggerSource() === ScriptApp.TriggerSource.TIME_BASED) {
      ScriptApp.deleteTrigger(t);
    }
  });
}

// ====================================
// 通常時データ収集
// ====================================
function collectNormalData(sheet, userId, sheetId, sheetName) {
  try {
    const rawData = sheet.getRange(CONFIG.NORMAL_RANGE).getValues();
    const results = [];
    const collectTime = new Date();
    for (let i = 0; i < rawData.length; i += CONFIG.BLOCK_SIZE) {
      const row = 86 + i;
      const blockData = rawData.slice(i, i + CONFIG.BLOCK_SIZE);
      const hasData = (blockData[0][1] && blockData[0][1] !== '・') || (blockData[0][2] && blockData[0][2] !== '・') || (blockData[0][3] && blockData[0][3] !== '・') || (blockData[0][35] && blockData[0][35] !== '');
      if (!hasData) continue;
      const blockNumber = Math.floor(i / CONFIG.BLOCK_SIZE) + 1;
      const paddedData = padArray(blockData[0], CONFIG.NORMAL_COLS);
      const dataRow = [collectTime, userId, sheetId, sheetName, row, blockNumber, ...paddedData];
      results.push(dataRow);
    }
    if (results.length >= 20) {
      const quality = evaluateDataQuality(results);
      results.forEach(row => {
        row.push(quality.totalG, quality.voiceCount, quality.voiceQuality, quality.czCount, quality.czFailureCount, quality.czQuality, quality.modeUsage, quality.stampUsage, quality.overallQuality);
      });
    } else {
      results.forEach(row => { row.push('', '', '評価中', '', '', '評価中', '評価中', '評価中', '評価中'); });
    }
    return results;
  } catch (error) {
    console.log(`❌ 通常時データ収集エラー: ${sheetName} - ${error}`);
    return [];
  }
}

// ====================================
// ATデータ収集
// ====================================
function collectATData(sheet, userId, sheetId, sheetName) {
  try {
    const rawData = sheet.getRange(CONFIG.AT_RANGE).getValues();
    const results = [];
    const collectTime = new Date();
    for (let i = 0; i < rawData.length; i += CONFIG.AT_BLOCK_SIZE) {
      const row = 86 + i;
      const blockData = rawData.slice(i, i + CONFIG.AT_BLOCK_SIZE);
      if ((!blockData[0][0] || blockData[0][0] === '') && (!blockData[0][1] || blockData[0][1] === '・')) continue;
      const blockNumber = Math.floor(i / CONFIG.AT_BLOCK_SIZE) + 1;
      const paddedData = padArray(blockData[0], CONFIG.AT_COLS);
      const dataRow = [collectTime, userId, sheetId, sheetName, row, blockNumber, ...paddedData];
      results.push(dataRow);
    }
    return results;
  } catch (error) {
    console.log(`❌ ATデータ収集エラー: ${sheetName} - ${error}`);
    return [];
  }
}

// ====================================
// ユーザーセグメント計算
// ====================================
function updateUserSegments(master) {
  const userSheet = master.getSheetByName('ユーザー管理');
  const normalDataSheet = master.getSheetByName('通常時データ集約');
  const users = userSheet.getDataRange().getValues();
  const normalData = normalDataSheet.getDataRange().getValues();
  for (let i = 1; i < users.length; i++) {
    const userId = users[i][0];
    const userData = normalData.filter(row => row[1] === userId);
    if (userData.length === 0) continue;
    const qualities = { voice: [], cz: [], mode: [], stamp: [] };
    userData.forEach(row => {
      if (row[48]) qualities.voice.push(row[48]);
      if (row[51]) qualities.cz.push(row[51]);
      if (row[52]) qualities.mode.push(row[52]);
      if (row[53]) qualities.stamp.push(row[53]);
    });
    if (qualities.voice.filter(q => q !== '評価中').length === 0) {
      userSheet.getRange(i + 1, 12, 1, 7).setValues([['評価中', '評価中', '評価中', '評価中', '-', '評価中', '評価中']]);
      continue;
    }
    const vQ = getMostFrequent(qualities.voice.filter(q => q !== '評価中'));
    const cQ = getMostFrequent(qualities.cz.filter(q => q !== '評価中'));
    const mU = getMostFrequent(qualities.mode.filter(q => q !== '評価中'));
    const sU = getMostFrequent(qualities.stamp.filter(q => q !== '評価中'));
    const createdDate = new Date(users[i][4]);
    const totalDays = Math.max(1, (new Date() - createdDate) / (1000 * 60 * 60 * 24));
    const frequency = (users[i][7] || 0) / totalDays;
    const freqLevel = frequency >= 0.5 ? '高' : frequency >= 0.2 ? '中' : '低';
    const aCount = [vQ, cQ, mU, sU].filter(q => q === 'A').length;
    const cCount = [vQ, cQ, mU, sU].filter(q => q === 'C').length;
    const dataAcc = (aCount >= 3 && cCount === 0) ? '高' : (aCount >= 2 || (aCount >= 1 && cCount <= 1)) ? '中' : '低';
    let segment = 'D';
    if (dataAcc === '高' && freqLevel === '高') segment = 'S';
    else if ((dataAcc === '高' && freqLevel === '中') || (dataAcc === '中' && freqLevel === '高')) segment = 'A';
    else if (dataAcc === '高' || freqLevel === '高' || (dataAcc === '中' && freqLevel === '中')) segment = 'B';
    else if (dataAcc !== '低' || freqLevel !== '低') segment = 'C';
    userSheet.getRange(i + 1, 12, 1, 7).setValues([[vQ, cQ, mU, sU, freqLevel, dataAcc, segment]]);
  }
}

function getMostFrequent(arr) {
  if (arr.length === 0) return '評価中';
  const counts = {}; arr.forEach(item => { counts[item] = (counts[item] || 0) + 1; });
  let max = 0; let res = arr[0];
  for (let k in counts) { if (counts[k] > max) { max = counts[k]; res = k; } }
  return res;
}

function evaluateDataQuality(results) {
  const totalG = results.reduce((sum, row) => sum + (parseInt(row[6]) || 0), 0);
  const voiceCount = results.reduce((sum, row) => {
    const memo = (row[36] || '').toString();
    const voices = (memo.match(/精一杯|適切な|化け物|ナイス|自分で/g) || []).length;
    return sum + voices;
  }, 0);
  const expectedVoice = totalG / 100;
  const voiceRate = expectedVoice > 0 ? voiceCount / expectedVoice : 0;
  const voiceQuality = voiceRate >= 0.7 ? 'A' : voiceRate >= 0.4 ? 'B' : 'C';
  let czCount = 0; let czFailureCount = 0;
  results.forEach(row => {
    const czData = row.slice(18, 33);
    for (let i = 0; i < czData.length; i += 3) {
      const val = (czData[i]||'').toString();
      if (val && val !== '・') {
        czCount++;
        if (val.includes('✖️') || val.includes('他人CZスルー')) czFailureCount++;
      }
    }
  });
  const czFailureRate = czCount > 0 ? czFailureCount / czCount : 0;
  const czQuality = czFailureRate >= 0.3 ? 'A' : czFailureRate >= 0.1 ? 'B' : 'C';
  const modeFilledCount = results.filter(row => row[44]).length;
  const modeUsageRate = results.length > 0 ? modeFilledCount / results.length : 0;
  const modeUsage = modeUsageRate >= 0.8 ? 'A' : modeUsageRate >= 0.5 ? 'B' : 'C';
  const stampUsedCount = results.filter(row => row[39]).length;
  const stampUsageRate = results.length > 0 ? stampUsedCount / results.length : 0;
  const stampUsage = stampUsageRate >= 0.5 ? 'A' : stampUsageRate >= 0.2 ? 'B' : 'C';
  const qualities = [voiceQuality, czQuality, modeUsage, stampUsage];
  const aCount = qualities.filter(q => q === 'A').length;
  const cCount = qualities.filter(q => q === 'C').length;
  let overallQuality = (aCount >= 3 && cCount === 0) ? '高' : (aCount >= 2 || (aCount >= 1 && cCount <= 1)) ? '中' : '低';
  return { totalG, voiceCount, voiceQuality, czCount, czFailureCount, czQuality, modeUsage, stampUsage, overallQuality };
}

// ====================================
// モード推測再計算 (Ver 5.3.0 Logic)
// ====================================
function recalculateMasterModes() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName('通常時データ集約');
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return;
  const data = sheet.getRange(2, 1, lastRow - 1, 48).getValues(); 
  const results = [];
  let prevKey = ''; let sheetHistory = [];
  for (let i = 0; i < data.length; i++) {
    const row = data[i];
    const key = row[2] + '_' + row[3];
    if (key !== prevKey) { sheetHistory = []; prevKey = key; }
    const res = calculateMasterRow(row, sheetHistory);
    sheetHistory.push(res);
    results.push(res);
  }
  const specialOut = results.map(r => [r.specialText]);
  const modeOut = results.map(r => [r.modeText]);
  sheet.getRange(2, 58, specialOut.length, 1).setValues(specialOut);
  sheet.getRange(2, 59, modeOut.length, 1).setValues(modeOut);
}

function calculateMasterRow(row, history) {
  const currentBlock = {
    pt: (row[7] || '').toString(),
    week: (row[8] || '').toString(),
    event1: (row[10] || '').toString(),
    event2: (row[11] || '').toString(),
    at: (row[15] || '').toString(),
    memo: (row[36] || '') + ',' + (row[37] || '')
  };
  if (!currentBlock.pt && !currentBlock.event1 && !currentBlock.at) {
    return { currentProbs: null, modeText: '', specialText: '', raw: currentBlock };
  }
  let eventType = 1;
  if (history.length > 0) {
    const prev = history[history.length - 1].raw;
    if (prev.at && prev.at !== '・' && prev.at !== '-') eventType = 1;
    else if (prev.event1.includes('革命BONUS') || prev.event2.includes('革命BONUS')) eventType = 1;
    else if (prev.event1.includes('決戦BONUS') || prev.event1.includes('CZ')) eventType = 2;
  }
  const priorProbs = getPriorProbsFromHistory(history.length, eventType, history);
  let currentProbs = { ...priorProbs };
  const modeProbsHistory = [];
  for (let k = 0; k < currentBlock.pt.length; k++) {
    currentProbs = bayesUpdate(currentProbs, currentBlock.pt[k], k + 1);
    modeProbsHistory.push({ ...currentProbs });
  }
  if (currentBlock.week.includes('周期')) {
    const m = currentBlock.week.match(/(\d+)周期/);
    if(m) currentProbs = bayesUpdateCeiling(currentProbs, parseInt(m[1]));
  }
  currentProbs = applyPerformanceBonus(currentProbs, currentBlock.memo);
  const prevWas600 = history.length > 0 && (history[history.length-1].raw.pt || '').includes('⑥');
  const specialProb = calculateSpecialTableProb(currentBlock.pt, modeProbsHistory, prevWas600);
  const p = currentProbs;
  return {
    currentProbs: p,
    modeText: `A${Math.round(p.A*100)}% B${Math.round(p.B*100)}% C${Math.round(p.C*100)}% 天国${Math.round(p.H*100)}%`,
    specialText: specialProb !== null ? `特殊${Math.round(specialProb*100)}%` : '',
    raw: currentBlock
  };
}

function getPriorProbsFromHistory(index, eventType, results) {
  if (eventType === 1 || index === 0) return { A: 0.69, B: 0.25, C: 0.05, H: 0.01 };
  const prevResult = results[results.length - 1];
  if (!prevResult || !prevResult.currentProbs) return { A: 0.69, B: 0.25, C: 0.05, H: 0.01 };
  const p = prevResult.currentProbs;
  let nextA = p.A * 0.66 + p.H * 0.66;
  let nextB = p.A * 0.29 + p.B * 0.66 + p.C * 0.32 + p.H * 0.31;
  let nextC = p.A * 0.04 + p.B * 0.32 + p.C * 0.57 + p.H * 0.02;
  let nextH = p.A * 0.01 + p.B * 0.02 + p.C * 0.43 + p.H * 0.01;
  const total = nextA + nextB + nextC + nextH;
  return { A: (nextA/total)*0.9, B: (nextB/total) + (nextA/total)*0.1, C: nextC/total, H: nextH/total };
}

function bayesUpdate(probs, pt, cycleNum) {
  const map = {'①':0,'②':1,'③':2,'④':3,'⑤':4,'⑥':5};
  const idx = map[pt]; if(idx===undefined) return probs;
  const l = [{A:.25,B:.29,C:.35,H:.05},{A:.23,B:.13,C:.44,H:.95},{A:.05,B:.16,C:.03,H:0},{A:.28,B:.12,C:.18,H:0},{A:.04,B:.14,C:0,H:0},{A:.15,B:.16,C:0,H:0}][idx];
  let pA=probs.A, pB=(cycleNum<4)?probs.B:0, pC=(cycleNum<6)?probs.C:0, pH=(cycleNum<2)?probs.H:0;
  const postA=l.A*pA, postB=l.B*pB, postC=l.C*pC, postH=l.H*pH;
  const total = postA+postB+postC+postH;
  return total===0 ? probs : { A:postA/total, B:postB/total, C:postC/total, H:postH/total };
}

function bayesUpdateCeiling(probs, weekNum) {
  const l = { A:[0,.14,.29,.04,.15,.13,.23][weekNum]||0, B:[0,.05,.13,.82,0,0,0][weekNum]||0, C:[0,.11,.18,.14,.12,.45,0][weekNum]||0, H:[0,1,0,0,0,0,0][weekNum]||0 };
  const postA=l.A*probs.A, postB=l.B*probs.B, postC=l.C*probs.C, postH=l.H*probs.H;
  const total = postA+postB+postC+postH;
  return total===0 ? probs : { A:postA/total, B:postB/total, C:postC/total, H:postH/total };
}

function applyPerformanceBonus(probs, memoText) {
  let p = { ...probs };
  if (memoText.includes('🟪機体') || memoText.includes('🟣ハルト')) p.A *= 0.02;
  if (memoText.includes('🟣ソウイチ')) return { A: 0, B: 0, C: 0, H: 1.0 };
  const countHigh = (memoText.match(/化け物|ec-白ピノ|ec-白/g) || []).length;
  for (let i = 0; i < countHigh; i++) p = updateWithLikelihood(p, 0.2, 0.8);
  const countLow = (memoText.match(/ナイス|自分で|🟦機体|🟥機体|ec-赤|ec-黒ピノ/g) || []).length;
  for (let i = 0; i < countLow; i++) p = updateWithLikelihood(p, 0.8, 1.0);
  const total = p.A + p.B + p.C + p.H;
  return total === 0 ? probs : { A: p.A/total, B: p.B/total, C: p.C/total, H: p.H/total };
}

function updateWithLikelihood(probs, lA, lOthers) {
  return { A: probs.A * lA, B: probs.B * lOthers, C: probs.C * lOthers, H: probs.H * lOthers };
}

function calculateSpecialTableProb(ptSequence, modeProbsHistory, prevEventWas600) {
  if (ptSequence.includes('⑤') || ptSequence.includes('⑥')) return 0;
  if (ptSequence.length < 1 || prevEventWas600) return null;
  const distNormal = { A:{'①':.25,'②':.23,'③':.05,'④':.28}, B:{'①':.29,'②':.13,'③':.16,'④':.12}, C:{'①':.35,'②':.44,'③':.03,'④':.18} };
  const distSpecial = { A:{'①':.16,'②':.31,'③':.06,'④':.47}, B:{'①':.20,'②':.32,'③':.31,'④':.17}, C:{'①':.38,'②':.39,'③':.03,'④':.20} };
  let sProb = 0.05;
  for (let i = 0; i < ptSequence.length; i++) {
    if (i === 0) continue;
    const pt = ptSequence[i]; const mP = modeProbsHistory[i]; if (!mP) continue;
    const ln = (distNormal.A[pt]||0)*mP.A + (distNormal.B[pt]||0)*mP.B + (distNormal.C[pt]||0)*mP.C;
    const ls = (distSpecial.A[pt]||0)*mP.A + (distSpecial.B[pt]||0)*mP.B + (distSpecial.C[pt]||0)*mP.C;
    const den = ls * sProb + ln * (1 - sProb);
    if (den !== 0) sProb = (ls * sProb) / den;
  }
  return sProb;
}