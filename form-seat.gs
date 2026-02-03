/**
 * ヴァルヴレイヴ2 配布自動化システム
 * Ver 3.0 (Final Distribution & Custom Mail)
 */

const CONFIG = {
  TEMPLATE_ID: 'XXXXXXXXXXXXXXXX', // Ver 8.4のID
  MASTER_ID: 'XXXXXXXXXXXXXXXX',   // 新・マスター管理
  
  // 列番号
  COL_NAME: 2,     // B: アカウント名
  COL_USERNAME: 3, // C: ユーザー名 (@xxx)
  COL_EMAIL: 4,    // D: メールアドレス
  COL_STATUS: 9,   // I: ステータス
  COL_USERID: 12,  // L: ユーザーID
  COL_URL: 14      // N: シートURL
};

function onOpen() {
  SpreadsheetApp.getUi().createMenu('管理者メニュー')
    .addItem('1. シート作成＆登録 (ステータス:OK)', 'createAndRegisterSheets')
    .addItem('2. 招待メール送信 (ステータス:設定完了)', 'inviteUsers')
    .addToUi();
}

function createAndRegisterSheets() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getActiveSheet();
  const data = sheet.getDataRange().getValues();
  const masterSS = SpreadsheetApp.openById(CONFIG.MASTER_ID);
  const userManageSheet = masterSS.getSheetByName('ユーザー管理');
  
  let lastIdNum = 0;
  userManageSheet.getDataRange().getValues().forEach(r => {
    const match = (r[0]||'').toString().match(/USER_gn_(\d+)/);
    if(match) lastIdNum = Math.max(lastIdNum, parseInt(match[1]));
  });
  
  let processCount = 0;
  
  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    const status = row[CONFIG.COL_STATUS - 1];
    const email = row[CONFIG.COL_EMAIL - 1];
    const name = row[CONFIG.COL_NAME - 1];
    const userName = row[CONFIG.COL_USERNAME - 1]; // @xxx
    
    if (status === 'OK' && !row[CONFIG.COL_URL - 1]) {
      try {
        lastIdNum++;
        const newId = 'USER_gn_' + ('000' + lastIdNum).slice(-3);
        // ファイル名変更
        const fileName = `【${name} 専用】XXXXXXXXXXXXXXXX [${userName}]`;
        
        const templateFile = DriveApp.getFileById(CONFIG.TEMPLATE_ID);
        const newFile = templateFile.makeCopy(fileName);
        const newUrl = newFile.getUrl();
        const newSpreadsheet = SpreadsheetApp.openById(newFile.getId());
        
        const sysSheet = newSpreadsheet.getSheetByName('_system');
        if (sysSheet) {
          sysSheet.getRange('B1').setValue(newId);
          sysSheet.getRange('B2').setValue(email);
          sysSheet.getRange('B3').setValue(new Date());
          sysSheet.getRange('B4').setValue(name);
        }
        
        userManageSheet.appendRow([newId, name, email, newUrl, new Date(), '', 'アクティブ']);
        
        sheet.getRange(i + 1, CONFIG.COL_USERID).setValue(newId);
        sheet.getRange(i + 1, CONFIG.COL_URL).setValue(newUrl);
        sheet.getRange(i + 1, CONFIG.COL_STATUS).setValue('作成済(設定待ち)');
        
        processCount++;
        
      } catch (e) {
        console.log(`エラー(${name}): ${e.toString()}`);
        sheet.getRange(i + 1, CONFIG.COL_STATUS).setValue('エラー: ' + e.message);
      }
    }
  }
  SpreadsheetApp.getUi().alert(`${processCount}件のシートを作成しました。\nURLを開いてトリガー設定を行ってください。`);
}

function inviteUsers() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getActiveSheet();
  const data = sheet.getDataRange().getValues();
  let inviteCount = 0;
  
  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    const status = row[CONFIG.COL_STATUS - 1];
    const email = row[CONFIG.COL_EMAIL - 1];
    const url = row[CONFIG.COL_URL - 1];
    const name = row[CONFIG.COL_NAME - 1];
    
    if (status === '設定完了' && url && email) {
      try {
        const fileIdMatch = url.match(/\/d\/([a-zA-Z0-9-_]+)/);
        if (fileIdMatch) {
          const file = DriveApp.getFileById(fileIdMatch[1]);
          
          // 権限付与（通知メールあり）
          file.addEditor(email);
          
          // 別途案内メール送信
          const subject = "【配布】ヴァルヴレイヴ2 実戦入力シートのご案内";
          const body = `${name} 様\n\n`
            + `この度は申請ありがとうございます。\n`
            + `あなた専用の実戦入力シートを作成いたしました。\n\n`
            + `以下のURLよりアクセスしてご利用ください。\n`
            + `▼専用シートURLXXXXXXXXXXXXXXXX
            + `【⚠️ 重要：最初にお読みください】\n`
            + `シートの全機能（自動計算・分析など）を利用するには、初回のみ「権限の承認（許可）」が必要です。\n\n`
            + `手順:\n`
            + `1. シートを開き、P80セルに「実行」と入力してください。\n`
            + `2. 「承認が必要です」という画面が出た場合、以下のガイドに従って許可してください。\n`
            + `※認証を促す画面が出ない場合は、下記の手順は不要でそのまま全機能をお使いいただけます。\n`
            + `👉 セットアップガイド: XXXXXXXXXXXXXXXX`
            + `【使い方のポイント】\n`
            + `* マスターシート: 「Ver.○○ マスター必ずコピーして使う」という名前のシートだけは、絶対に直接入力しないで「マスターをコピーして」複製したものをご利用ください。マスターさえ無事なら、コピーしたシートが壊れても問題ありません。また、初回はあらかじめ白紙シートを設けているので、そこから使っていただいても問題ありません（デフォでついてる白紙シートはそのまま記入してOKです）\n`
            + `  ▶️シートをコピーする XXXXXXXXXXXXXXXX`
            + `* 1稼働で1シート: 1回の稼働、つまり「○月○日777番台」で1枚のシートが、基本の使い方になります。シート名は必ず好きな名前に変更するようにしてください。よくあるのは「0117店名台番」とか「店名456確」等です。\n\n`
            + `* Q&A: 詳細な使い方の説明ページを鋭意製作中ですが、まだできてません。申し訳ありません！ 当面のご質問は、私のポストで【重要】ヴァル2シートQ&Aというポストを用意しますので、そこにリプライしていただくか、DMをください。\n\n`
            + `【重要】スプレッドシートを初めてご利用の方へ\n`
            + `本ツールはGoogleスプレッドシート公式アプリでの使用を前提としています。\n`
            + `ブラウザ版では動作が重くなるため、必ずアプリをインストールしてご利用ください。\n`
            + `* iPhone (iOS): XXXXXXXXXXXXXXXX
            + `* Android: XXXXXXXXXXXXXXXX
            + `＜次回予告＞\n`
            + `ヴァル2以外にも脳汁出せそうな主要機種については、皆さんと一緒に遊びたいのでシートリリースを考えています。また、分析データの発信や、実際の456データなどをみんなで共有できる、ユーザー限定コミュニティを開設検討中です。またご案内します。\n\n`
            + `では、皆様の勝利を願っております！\n\n`
            + `--------------------------------------------------\n`
            + `Valvrave II_recordnotes\n`
            + `© XXXXXXXXXXXXXXXX All rights reserved.`;
            
          MailApp.sendEmail(email, subject, body);
          
          sheet.getRange(i + 1, CONFIG.COL_STATUS).setValue('完了');
          inviteCount++;
        }
      } catch (e) {
        console.log(`招待エラー(${email}): ${e.toString()}`);
        sheet.getRange(i + 1, CONFIG.COL_STATUS).setValue('招待エラー');
      }
    }
  }
  SpreadsheetApp.getUi().alert(`${inviteCount}件の招待メールを送信しました。`);
}