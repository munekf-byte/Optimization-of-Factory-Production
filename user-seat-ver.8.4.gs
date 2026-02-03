/**
 * ヴァルヴレイヴ2 データ分析システム
 * Ver 8.4 (Valvrave: Resonance) 
 * @OnlyCurrentDoc
 */

function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('V2システム')
    .addItem('🚀 配布用トリガー設定', 'setupTriggerOnly')
    .addSeparator()
    .addItem('データ更新・計算実行', 'runUpdate')
    .addSeparator()
    .addItem('モード推定: 計算実行', 'calculateAllModes')
    .addSeparator()
    .addSubMenu(ui.createMenu('メンテナンス')
      .addItem('全データ集計', 'aggregateAllData')
      .addItem('システム初期化', 'setupSystemSheets')
      .addItem('ヘッダー再設定', 'resetAllHeaders'))
    .addToUi();
  checkWarningPopup();
}

function checkWarningPopup() {
  const properties = PropertiesService.getUserProperties();
  const lastWarningDate = properties.getProperty('lastWarningDate');
  const today = new Date().toDateString();
  if (!lastWarningDate || isMoreThan7DaysAgo(lastWarningDate)) {
    let userName = '利用者';
    try {
      const sys = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('_system');
      if (sys) { const n = sys.getRange('B4').getValue(); if(n) userName = n; }
    } catch(e){}
    Utilities.sleep(500);
    SpreadsheetApp.getUi().alert('⚠️ 利用規約の確認',`${userName} 様\n\n専用発行されたデータ管理システムです。\n\n【禁止事項】\n・複製及び第三者への譲渡\n・システムの流用・改変\n\n※ご自身の入力データのSNS公開や分析は自由です。\n規約違反が確認された場合、規約に準じた措置を取らせていただきます。\n\n--------------------------------------------------\nValvrave II_recordnotes\n© @puchun_dobadoba All rights reserved.`,SpreadsheetApp.getUi().ButtonSet.OK);
    properties.setProperty('lastWarningDate', today);
  }
}
function isMoreThan7DaysAgo(d){const l=new Date(d),t=new Date();return Math.ceil(Math.abs(t-l)/86400000)>=7;}

function handleEdit(e) {
  const range = e.range;
  const sheet = e.source.getActiveSheet();
  if (sheet.getName() === '革命の軌跡' && range.getA1Notation() === 'F3' && range.getValue() === '更新') {
    range.setValue('集計中...'); SpreadsheetApp.flush();
    aggregateAllData(); // 高速全更新
    range.setValue('完了'); sheet.getRange('G3').setValue(Utilities.formatDate(new Date(),'JST','MM/dd HH:mm'));
  }
  if (range.getA1Notation() === 'P80' && (range.getValue()==='実行'||range.getValue()===true)) {
    calculateBalance();
    calculateAllModes();
    range.setValue('');
  }
}

function runUpdate() {
  const lock = LockService.getScriptLock();
  // 30秒待機（他の処理が終わるのを待つ）
  if (lock.tryLock(30000)) {
    try {
      calculateBalance();
      aggregateAllData();
    } catch (e) {
      console.error(e);
      SpreadsheetApp.getActiveSpreadsheet().toast("エラーが発生しました: " + e.message);
    } finally {
      lock.releaseLock();
    }
  } else {
    SpreadsheetApp.getActiveSpreadsheet().toast("処理中です。しばらくお待ちください。", "混雑中", 5);
  }
}

// ====================================
// (Ver 8.1 Fix)
// ====================================
function calculateBalance() {
  try {
    const sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
    const startDiff = (sheet.getRange('T81').getValue() || 0);
    let myDiff = 0;
    
    const normRange = sheet.getRange('DZ86:FO196').getValues();
    const czG_Range = sheet.getRange('CP86:CP196').getValues();
    const atRange = sheet.getRange('FQ86:GJ384').getValues();
    const retRange = sheet.getRange('GI86:GI384').getValues();
    const zRange = sheet.getRange('Z86:Z196').getValues();
    
    const normResults_User = []; 
    const normResults_Hidden = [];
    const atResults_User = []; 
    const atResults_Hidden = [];
    
    const extractNum = (val) => {
      if (typeof val === 'number') return val;
      const str = (val || '').toString();
      if (str === '・') return 0;
      const match = str.match(/\d+/);
      return match ? parseInt(match[0]) : 0;
    };
    
    const atMap = {};
    let currentAtNum = '';
    
    for (let i = 0; i < atRange.length; i += 2) {
      let atNum = (atRange[i][0] || '').toString().trim();
      if (atNum) currentAtNum = atNum;
      else if (currentAtNum && atRange[i][1]) atNum = currentAtNum;
      if (atNum) {
        if (!atMap[atNum]) atMap[atNum] = [];
        atMap[atNum].push({ idx: i, row: atRange[i] });
      }
    }
  
    for (let i = 0; i < normRange.length; i += 3) {
      const r = normRange[i];
      const czG = (czG_Range[i][0] || 0);
      
      if (!r[0] && !r[4] && !r[9]) {
        normResults_User.push(['']); normResults_User.push(['']); normResults_User.push(['']);
        normResults_Hidden.push(['']); normResults_Hidden.push(['']); normResults_Hidden.push(['']);
        continue;
      }
      
      // ①通常G消費
      const normG = (r[0] || 0);
      const consume = normG * 1.529;
      myDiff -= consume;
      
      // ★ここでスタンプ（当選の瞬間）を確定
      const hitBalance = Math.round(startDiff + myDiff);
      
      // 次の計算へ（CZ消費）
      myDiff -= (czG * 1.529);
      
      // ②ボーナス獲得
      const ev1 = (r[4] || '').toString();
      const ev2 = (r[5] || '').toString();
      const at = r[9];
      let bonusGet = 0;
      
      if (ev1.includes('革命BONUS') || ev2.includes('革命BONUS')) {
        const inputBonus = extractNum(zRange[i+2][0]); 
        bonusGet = (inputBonus > 0) ? inputBonus : ((at) ? 539 : 468);
      } else if (ev1.includes('決戦BONUS') || ev2.includes('決戦BONUS')) {
        bonusGet = 72;
      }
      myDiff += bonusGet;
      
      // ③AT計算
      const atKey = (at || '').toString().trim();
      if (atKey && atMap[atKey]) {
        const rounds = atMap[atKey];
        rounds.forEach((round, rIdx) => {
          const d = round.row;
          
          // ハラキリ消費
          if (rIdx > 0) myDiff -= (3.3 * 1.529);
          
          // 純増
          let g = 0;
          g += extractNum(d[1]);
          g += extractNum(d[6]);
          g += extractNum(d[19]);
          
          let gain = g * 9;
          
          // 特殊処理 (ED or 引戻し)
          // ※GI列は retRange[round.idx][0]
          const rawRet = retRange[round.idx][0];
          const retVal = extractNum(rawRet);
          
          if (d[5] === '切断║ED') {
            gain += 200; // ED恩恵
            if (retVal > 0) {
               gain -= (retVal * 9); // 残りG数分減算
               console.log(`AT${atKey} R${rIdx+1}: ED残り ${retVal}G 減算`);
            }
          } else {
            // 通常時: 引戻し消費
            if (retVal > 0) {
               myDiff -= (retVal * 1.529);
               console.log(`AT${atKey} R${rIdx+1}: 引戻し ${retVal}G 消費`);
            }
          }
          
          myDiff += gain;
          round.diff = Math.round(startDiff + myDiff);
        });
        
        // ※ループ後の「最終行チェック」は削除（ループ内で処理するため）
      }
      
      // 書き込み用配列 (3行目に入れる)
      // ★ここで hitBalance を使う！
      normResults_User.push(['']); 
      normResults_User.push(['']); 
      normResults_User.push([hitBalance]);
      
      normResults_Hidden.push([hitBalance]);
      normResults_Hidden.push(['']);
      normResults_Hidden.push(['']);
    }
    
    for (let i = 0; i < atRange.length; i += 2) {
      let val = '';
      for (let k in atMap) {
        const found = atMap[k].find(item => item.idx === i);
        if (found && found.diff !== undefined) {
          val = found.diff; break;
        }
      }
      atResults_User.push([val]); atResults_User.push(['']);
      atResults_Hidden.push([val]); atResults_Hidden.push(['']);
    }
    
    // 書き込み実行
    console.log('書き込み開始...');
    
    if (normResults_User.length > 0) {
      const rangeBP = sheet.getRange(86, 68, normResults_User.length, 1);
      rangeBP.clearContent();
      rangeBP.setValues(normResults_User);
      
      const rangeFH = sheet.getRange(86, 164, normResults_Hidden.length, 1);
      rangeFH.setValues(normResults_Hidden);
    }
    
    if (atResults_User.length > 0) {
      const rangeQ = sheet.getRange(203, 17, atResults_User.length, 1);
      rangeQ.clearDataValidations();
      rangeQ.setValues(atResults_User);

      const rangeGK = sheet.getRange(86, 193, atResults_Hidden.length, 1);
      rangeGK.setValues(atResults_Hidden);
    }
    
    sheet.getRange('V81').setValue(Math.round(myDiff));
    sheet.getRange('X81').setValue(Math.round(startDiff + myDiff));
    console.log('差枚計算完了');

  } catch (e) {
    console.error('エラー発生:', e.stack);
    SpreadsheetApp.getUi().alert('計算エラー: ' + e.message);
  }
}


// ====================================
// 全データ集計処理 (Ver 8.4 Fix: Correct Cell Reference)
// ====================================
function aggregateAllData() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let db = ss.getSheetByName('_集計データ');
  if(!db) { setupSystemSheets(); db=ss.getSheetByName('_集計データ'); }
  
  if (db.getMaxRows() < 200) db.insertRowsAfter(db.getMaxRows(), 200);

  const newData = { norm:[], at:[], trace:[], summary:[] };
  const exclude = ['集計','革命の軌跡','_集計データ','_system'];
  const sheets = ss.getSheets();
  let cnt = 0;
  
  // 【修正】一括読み込み範囲 (DG108, DG103, DG106, CY97, CY84, DG105)
  const summaryRanges = ['CO115', 'CO92', 'AC81', 'AE81', 'V81', 'CN82', 'CN84', 'DG108', 'DG103', 'DG106', 'CY97', 'CY84', 'DG105'];
  
  for(let i=0; i<sheets.length; i++) {
    const s = sheets[i];
    const sName = s.getName();
    if(exclude.includes(sName)||sName.startsWith('_')) continue;
    
    const isTarget = s.getRange('D78').getValue();
    if (isTarget === false) continue;
    
    const sId = s.getSheetId();
    cnt++;
    
    // データ一括取得
    const nRange = s.getRange('DZ86:FO196').getValues();
    const aRange = s.getRange('FQ86:GJ384').getValues();
    const tRange = s.getRange('AY23:BG47').getValues();
    const biGainRange = s.getRange('BI23:BI47').getValues(); 
    
    // サマリー値取得
    const sumValues = summaryRanges.map(a1 => s.getRange(a1).getValue());
    
    // 通常時
    for(let j=0; j<nRange.length; j++) {
      if(!nRange[j][0] && !nRange[j][4] && !nRange[j][9]) continue;
      newData.norm.push([sName, sId, 86+j, ...nRange[j]]);
    }
    
    // AT
    for(let j=0; j<aRange.length; j++) {
      if(!aRange[j][0] && (!aRange[j][1] || aRange[j][1]==='・')) continue;
      newData.at.push([sName, sId, 86+j, ...aRange[j]]);
    }
    
    // 軌跡
    for(let j=0; j<tRange.length; j++) {
      const r = tRange[j];
      const atN=r[0], trig=r[1], ch=r[2];
      let amt = biGainRange[j][0]; if(amt===''||amt===undefined) amt='-';
      if(!atN && (!trig||trig==='-') && (!ch||ch==='-')) continue;
      newData.trace.push([sName,sId,...r,amt]);
    }
    
    // イベントカウント
    const ev1 = nRange.map(r => (r[4]||'').toString());
    const ev2 = nRange.map(r => (r[5]||'').toString());
    const atCol = nRange.map(r => (r[9]||'').toString());
    let czC=0, czS=0, kakuC=0, kakuS=0, kessC=0, kessS=0;
    
    for(let r=0; r<nRange.length; r++) {
      if(!nRange[r][0] && !nRange[r][4] && !nRange[r][9]) continue;
      const e1 = ev1[r], e2 = ev2[r], at = atCol[r];
      const isCZ = (e1 === 'CZ');
      const isKaku = (e1.includes('革命BONUS') || e2.includes('革命BONUS'));
      const isKess = (e1.includes('決戦BONUS') || e2.includes('決戦BONUS'));
      const isAT = (at && at.toString().includes('AT'));
      const isSuccess = (e2.match(/革命|決戦|RUSH/)); 
      if(isCZ) { czC++; if(isSuccess) czS++; }
      if(isKaku) { kakuC++; if(isAT) kakuS++; }
      if(isKess) { kessC++; if(isAT) kessS++; }
    }
    
    // 【修正】サマリー追加 (sumValues[7]〜[12]を使用)
    newData.summary.push([
      sName, sId, 
      sumValues[0], sumValues[1], sumValues[2], sumValues[3], sumValues[4],
      sumValues[5], sumValues[6], 
      czC, czS, kakuC, kakuS, kessC, kessS, 
      sumValues[7], sumValues[8], sumValues[9], sumValues[10], sumValues[11], sumValues[12]
    ]);
  }
  
  // 書き込み
  db.getRange(86, 59, db.getMaxRows() - 85, 106).clearContent();
  
  if(newData.norm.length > 0) db.getRange(86, 59, newData.norm.length, 45).setValues(newData.norm);
  if(newData.at.length > 0) db.getRange(86, 104, newData.at.length, 23).setValues(newData.at);
  if(newData.trace.length > 0) db.getRange(86, 131, newData.trace.length, 12).setValues(newData.trace);
  
  if(newData.summary.length > 0) {
    db.getRange(86, 144, newData.summary.length, 21).setValues(newData.summary);
  }
  
  SpreadsheetApp.getActiveSpreadsheet().toast(`集計完了: ${cnt}シート`, '✅', 5);
}

function getLastRowInRange(s,sr,sc,nc){
  const v=s.getRange(sr,sc,s.getMaxRows()-sr+1,nc).getValues();
  for(let i=v.length-1;i>=0;i--) if(v[i].some(c=>c!==''&&c!=null)) return sr+i;
  return sr-1;
}

function setupSystemSheets() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let db = ss.getSheetByName('_集計データ');
  const old = ss.getSheetByName('集計');
  if(old && !db) { old.setName('_集計データ'); db=old; db.hideSheet(); }
  else if(!db) { db=ss.insertSheet('_集計データ'); db.hideSheet(); }
  setupAllHeaders(db);
  if(!ss.getSheetByName('革命の軌跡')) ss.insertSheet('革命の軌跡',0);
}

function setupAllHeaders(sheet) {
  const norm=['シート名','ID','行','実G','規定pt履歴','周期','契機','イベント1','イベント2','示唆','革ボ情報','革ボ獲得','AT初当り','AT実績','AT獲得枚数',
    '1-1','1-2','1-3','2-1','2-2','2-3','3-1','3-2','3-3','4-1','4-2','4-3','5-1','5-2','5-3','自動メモ','手書き1','特殊モード推定結果','自動計算差枚数記録（通常時）','モード推測結果','確定設定情報',
    '予備2','予備3','予備4','予備5','予備6','予備7','予備8','予備9','予備10'];
  sheet.getRange(85,59,1,norm.length).setValues([norm]).setFontWeight('bold').setBackground('#4a86e8').setFontColor('white');
  
  const at=['シート名','ID','行','AT番号','R種別','継続契機','状態','枚数記録','切断','道中乗せ','R画面','革剣1','革剣2','特殊枚数','R画面2','特殊演出','備考','枚数スタンプ','確定情報','AT予備2','AT予備3','AT予備4','AT予備5','AT中差枚数スタンプ'];
  sheet.getRange(85,104,1,at.length).setValues([at]).setFontWeight('bold').setBackground('#e69138').setFontColor('white');
  
  const tr=['シート名','ID','AT番号','RUSH契機','最終連チャン','[超]通常D','[超]究極','[超]全D','[大]通常D','[大]究極','[大]全D','獲得枚数'];
  sheet.getRange(85,131,1,tr.length).setValues([tr]).setFontWeight('bold').setBackground('#cc0000').setFontColor('white');
  
  // 【修正】収支サマリーヘッダー (LD成功数を追加)
  const sum=['シート名','ID','計算上差枚','実投資枚数','交換枚数','最終収支','理論差枚',
    '総G数','通常G数','CZ回数','CZ成功','革ボ回数','革ボ成功','決戦回数','決戦成功',
    'ニンゲン','LD','LDH','電脳回数','直撃回数','LD成功数'
  ];
  sheet.getRange(85,144,1,sum.length).setValues([sum]).setFontWeight('bold').setBackground('#20124d').setFontColor('white');
}
function resetAllHeaders(){ setupAllHeaders(SpreadsheetApp.getActiveSpreadsheet().getSheetByName('_集計データ')); }

// ... (calculateAllModes 以下のモード推定ロジックは Ver 7.8 のまま維持) ...
function calculateAllModes() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  const startRow = 86;
  const blockSize = 3;
  const areaC = sheet.getRange('DZ86:FO196').getValues();
  const initialThroughCount = parseInt(sheet.getRange('P83').getValue()) || 0;
  const results = [];
  for (let i = 0; i < areaC.length; i += blockSize) {
    const row = startRow + i;
    const blockData = areaC[i];
    if (!blockData[1] && !blockData[4] && !blockData[9]) {
      results.push(null);
      continue;
    }
    const result = processBlockInMemory(row, i, areaC, results, initialThroughCount);
    results.push(result);
  }
  writeBatchResults(sheet, startRow, results, blockSize);
  SpreadsheetApp.flush();
}

function processBlockInMemory(row, index, areaC, results, initialThroughCount) {
  const currentBlock = areaC[index];
  const eventType = getEventTypeFromAreaC(index, areaC);
  const priorProbs = getPriorProbsFromHistory(index, eventType, results, initialThroughCount);
  const ptSequence = (currentBlock[1] || '').toString();
  let currentProbs = { ...priorProbs };
  const modeProbsHistory = [];
  for (let i = 0; i < ptSequence.length; i++) {
    const pt = ptSequence[i];
    const cycleNum = i + 1;
    currentProbs = bayesUpdate(currentProbs, pt, cycleNum);
    modeProbsHistory.push({ ...currentProbs });
  }
  const weekText = (currentBlock[2] || '').toString();
  if (weekText.includes('周期')) {
    const weekNum = parseInt(weekText.match(/(\d+)周期/)[1]);
    currentProbs = bayesUpdateCeiling(currentProbs, weekNum);
  }
  const memoText = (currentBlock[13] || '') + ',' + (currentBlock[14] || '');
  currentProbs = applyPerformanceBonus(currentProbs, memoText);
  const prevEventWas600 = checkPrev600(index, areaC);
  const specialProb = calculateSpecialTableProb(ptSequence, modeProbsHistory, prevEventWas600);
  return { eventType, priorProbs, currentProbs, specialProb };
}

function getEventTypeFromAreaC(index, areaC) {
  if (index === 0) return 1;
  const prevBlock = areaC[index - 3];
  if (!prevBlock) return 1;
  const prevEvent1 = (prevBlock[4] || '').toString();
  const prevEvent2 = (prevBlock[5] || '').toString();
  const prevAT = (prevBlock[9] || '').toString();
  if (prevAT && prevAT !== '・' && prevAT !== '-') return 1;
  if (prevEvent1.includes('革命BONUS') || prevEvent2.includes('革命BONUS')) return 1;
  if (prevEvent1.includes('決戦BONUS') || prevEvent2.includes('決戦BONUS') || prevEvent1.includes('CZ') || prevEvent2.includes('CZ')) return 2;
  return 1;
}

function checkPrev600(index, areaC) {
  if (index === 0) return false;
  const prevBlock = areaC[index - 3];
  if (!prevBlock) return false;
  const ptSeq = (prevBlock[1] || '').toString();
  return ptSeq.includes('⑥');
}

function getPriorProbsFromHistory(index, eventType, results, initialThroughCount) {
  if (index === 0) {
    let p = { A: 0.69, B: 0.25, C: 0.05, H: 0.01 };
    if (initialThroughCount > 0) for (let i = 0; i < initialThroughCount; i++) p = applyDecay(p);
    return p;
  }
  if (eventType === 1 || index < 3) return { A: 0.69, B: 0.25, C: 0.05, H: 0.01 };
  const prevResult = results[results.length - 1];
  if (!prevResult || !prevResult.currentProbs) return { A: 0.69, B: 0.25, C: 0.05, H: 0.01 };
  const p = prevResult.currentProbs;
  let nextA = p.A * 0.66 + p.B * 0 + p.C * 0 + p.H * 0.66;
  let nextB = p.A * 0.29 + p.B * 0.66 + p.C * 0.32 + p.H * 0.31;
  let nextC = p.A * 0.04 + p.B * 0.32 + p.C * 0.57 + p.H * 0.02;
  let nextH = p.A * 0.01 + p.B * 0.02 + p.C * 0.43 + p.H * 0.01;
  const total = nextA + nextB + nextC + nextH;
  return applyDecay({ A: nextA/total, B: nextB/total, C: nextC/total, H: nextH/total });
}

function applyDecay(p) {
  const decayFactor = 0.9;
  const lostA = p.A * (1 - decayFactor);
  return { A: p.A * decayFactor, B: p.B + lostA, C: p.C, H: p.H };
}

function bayesUpdate(probs, pt, cycleNum) {
  const ptMap = { '①': 0, '②': 1, '③': 2, '④': 3, '⑤': 4, '⑥': 5 };
  const ptIdx = ptMap[pt];
  if (ptIdx === undefined) return probs;
  const l = [{ A: 0.25, B: 0.29, C: 0.35, H: 0.05 }, { A: 0.23, B: 0.13, C: 0.44, H: 0.95 }, { A: 0.05, B: 0.16, C: 0.03, H: 0.00 }, { A: 0.28, B: 0.12, C: 0.18, H: 0.00 }, { A: 0.04, B: 0.14, C: 0.00, H: 0.00 }, { A: 0.15, B: 0.16, C: 0.00, H: 0.00 }][ptIdx];
  let pA=probs.A, pB=(cycleNum<4)?probs.B:0, pC=(cycleNum<6)?probs.C:0, pH=(cycleNum<2)?probs.H:0;
  const postA = l.A * pA, postB = l.B * pB, postC = l.C * pC, postH = l.H * pH;
  const total = postA + postB + postC + postH;
  return total === 0 ? probs : { A: postA/total, B: postB/total, C: postC/total, H: postH/total };
}

function bayesUpdateCeiling(probs, weekNum) {
  const l = { A: [0, 0.14, 0.29, 0.04, 0.15, 0.13, 0.23][weekNum] || 0, B: [0, 0.05, 0.13, 0.82, 0.00, 0.00, 0.00][weekNum] || 0, C: [0, 0.11, 0.18, 0.14, 0.12, 0.45, 0.00][weekNum] || 0, H: [0, 1.00, 0.00, 0.00, 0.00, 0.00, 0.00][weekNum] || 0 };
  const postA = l.A * probs.A, postB = l.B * probs.B, postC = l.C * probs.C, postH = l.H * probs.H;
  const total = postA + postB + postC + postH;
  if (total === 0) return probs;
  return { A: postA/total, B: postB/total, C: postC/total, H: postH/total };
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
  if (total === 0) return probs;
  return { A: p.A/total, B: p.B/total, C: p.C/total, H: p.H/total };
}

function updateWithLikelihood(probs, lA, lOthers) {
  return { A: probs.A * lA, B: probs.B * lOthers, C: probs.C * lOthers, H: probs.H * lOthers };
}

function calculateSpecialTableProb(ptSequence, modeProbsHistory, prevEventWas600) {
  if (ptSequence.includes('⑤') || ptSequence.includes('⑥')) return 0;
  if (ptSequence.length < 1 || prevEventWas600) return null;
  const distNormal = { A: { '①': 0.25, '②': 0.23, '③': 0.05, '④': 0.28 }, B: { '①': 0.29, '②': 0.13, '③': 0.16, '④': 0.12 }, C: { '①': 0.35, '②': 0.44, '③': 0.03, '④': 0.18 } };
  const distSpecial = { A: { '①': 0.16, '②': 0.31, '③': 0.06, '④': 0.47 }, B: { '①': 0.20, '②': 0.32, '③': 0.31, '④': 0.17 }, C: { '①': 0.38, '②': 0.39, '③': 0.03, '④': 0.20 } };
  let specialProb = 0.05;
  for (let i = 0; i < ptSequence.length; i++) {
    if (i === 0) continue;
    const pt = ptSequence[i]; const mP = modeProbsHistory[i]; if (!mP) continue;
    const ln = (distNormal.A[pt]||0)*mP.A + (distNormal.B[pt]||0)*mP.B + (distNormal.C[pt]||0)*mP.C;
    const ls = (distSpecial.A[pt]||0)*mP.A + (distSpecial.B[pt]||0)*mP.B + (distSpecial.C[pt]||0)*mP.C;
    const priorS = specialProb, priorN = 1 - specialProb;
    const denom = ls * priorS + ln * priorN;
    if (denom === 0) continue;
    specialProb = (ls * priorS) / denom;
  }
  return specialProb;
}

function writeBatchResults(sheet, startRow, results, blockSize) {
  const bg=[], bh=[], bl=[], cj=[], bq=[];
  results.forEach(res => {
    if (!res) {
      bg.push(['']); bh.push(['','','','']); bl.push(['','','','']); cj.push(['']); bq.push(['']);
    } else {
      const { eventType, priorProbs: pp, currentProbs: cp, specialProb: sp } = res;
      bg.push([eventType]);
      bh.push([pp.A, pp.B, pp.C, pp.H]);
      bl.push([cp.A, cp.B, cp.C, cp.H]);
      const txt = `A${Math.round(cp.A*100)}% B${Math.round(cp.B*100)}% C${Math.round(cp.C*100)}% 天国${Math.round(cp.H*100)}%`;
      cj.push([txt]);
      bq.push([sp !== null ? `特殊${Math.round(sp*100)}%` : '']);
    }
    for(let r=1; r<blockSize; r++) {
      bg.push(['']); bh.push(['','','','']); bl.push(['','','','']); cj.push(['']); bq.push(['']);
    }
  });
  
  const len = bg.length;
  sheet.getRange(startRow, 59, len, 1).setValues(bg);
  sheet.getRange(startRow, 60, len, 4).setValues(bh);
  sheet.getRange(startRow, 64, len, 4).setValues(bl);
  
  const richTexts = cj.map(row => {
    const text = (row[0] || '').toString();
    if(!text) return SpreadsheetApp.newRichTextValue().setText('').build();
    const builder = SpreadsheetApp.newRichTextValue().setText(text);
    const probs = text.match(/(\d+)%/g);
    if (probs) {
      const pVals = probs.map(s => parseInt(s));
      const maxP = Math.max(...pVals);
      ['A','B','C','天国'].forEach((m, i) => {
        if(pVals[i] === maxP) {
          const pStr = `${m}${pVals[i]}%`;
          const start = text.indexOf(pStr);
          if(start >= 0) builder.setTextStyle(start, start + pStr.length, SpreadsheetApp.newTextStyle().setBold(true).build());
        }
      });
    }
    return builder.build();
  });
  
  const richTextsForBN = new Array(len).fill(SpreadsheetApp.newRichTextValue().setText('').build());
  const valuesForBO = new Array(len).fill(['']);
  for(let i=0; i<richTexts.length; i+=blockSize) {
    if (i+2 < len) {
      richTextsForBN[i+2] = richTexts[i];
      valuesForBO[i+2] = bq[i];
    }
  }
  
  sheet.getRange(startRow, 66, len, 1).setRichTextValues(richTextsForBN.map(r => [r]));
  sheet.getRange(startRow, 67, len, 1).setValues(valuesForBO);
  sheet.getRange(startRow, 69, len, 1).setValues(bq);
}

function setupTriggerOnly() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const handlerName = 'handleEdit';
  const triggers = ScriptApp.getProjectTriggers();
  for (let i = 0; i < triggers.length; i++) {
    if (triggers[i].getHandlerFunction() === handlerName) {
      SpreadsheetApp.getUi().alert('✅ 設定済みです。\nそのままユーザーを招待してください。');
      return;
    }
  }
  ScriptApp.newTrigger(handlerName).forSpreadsheet(ss).onEdit().create();
  SpreadsheetApp.getUi().alert('✅ トリガー設定完了\n\nスマホでの自動更新が有効になりました。\nユーザーを招待してください。');
}