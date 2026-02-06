import asyncio
from playwright.async_api import async_playwright

async def run():
    async with async_playwright() as p:
        # ブラウザを手動操作するために表示(headless=False)で起動
        browser = await p.chromium.launch(headless=False)
        # 人間に近い設定でコンテキストを作成
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        print("\n--- 通行証発行プロセス ---")
        print("1. ブラウザで対象のページを開いてください。")
        print("2. マイナス数値（-3,070など）が表示されるまで待ってください。")
        print("3. 表示を確認したら、このターミナルに戻って Enter を押してください。")

        # ターゲットURLへ誘導
        await page.goto("https://min-repo.com/2879243/?kishu=all")

        # ユーザーが確認してEnterを押すまで待機
        input("\n[確認中...] 数値が見えたら、ここ（ターミナル）でEnterキーを叩いてください：")

        # 「人間」として認められた通信状態をファイルに保存
        await context.storage_state(path="minrepo_state.json")
        print("\n[成功] 通行証 'minrepo_state.json' を作成しました。")
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(run())