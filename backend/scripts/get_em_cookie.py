"""
东方财富 Cookie 获取脚本
使用方法: uv run python scripts/get_em_cookie.py
"""
from playwright.sync_api import sync_playwright


def get_eastmoney_cookie():
    """使用 Playwright 获取东方财富 Cookie"""
    with sync_playwright() as p:
        # 启动浏览器
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        try:
            # 访问东方财富行情页面
            page.goto("https://quote.eastmoney.com/center/gridlist.html", 
                     wait_until="domcontentloaded", timeout=30000)
            # 等待 3 秒让 JS 生成加密 Cookie
            page.wait_for_timeout(3000)

            # 获取所有 Cookie 并格式化为字符串
            cookies = context.cookies()
            cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
            return cookie_str
        except Exception as e:
            print(f"获取失败: {e}")
            return None
        finally:
            browser.close()


if __name__ == "__main__":
    print("=" * 60)
    print("正在启动浏览器获取 Cookie...")
    print("请在打开的页面中手动登录东方财富（如需要）")
    print("=" * 60)
    
    cookie_str = get_eastmoney_cookie()
    
    if cookie_str:
        print("\n" + "=" * 60)
        print("获取成功！请将以下内容复制/覆盖到 .carrotquant/secrets.env 文件中：\n")
        print(f'EM_COOKIE="{cookie_str}"\n')
        print("=" * 60)
    else:
        print("获取 Cookie 失败，请重试")
