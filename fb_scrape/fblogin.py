import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)

        context = await browser.new_context()
        page = await context.new_page()

        print("👉 Opened browser. Log into Facebook manually.")

        await page.goto("https://www.facebook.com/")

        # Wait until login is clearly complete
        await page.wait_for_url("**facebook.com/**", timeout=0)

        print("👉 After login, press ENTER here to save session...")
        input()

        # Save cookies + local storage
        await context.storage_state(path="fb_state.json")
        print("✅ Saved to fb_state.json")

        await browser.close()

asyncio.run(main())