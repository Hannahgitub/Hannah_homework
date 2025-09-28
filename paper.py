import time
import random
import pandas as pd
from datetime import datetime
from playwright.sync_api import sync_playwright

# 全局配置
BASE_URL = "https://www.tandfonline.com"
JOURNALS_LIST_URL = f"{BASE_URL}/action/showPublications?pubType=journal"
MAX_JOURNALS = 2  # 限制抓取的期刊数量，先从少量开始测试
MAX_ARTICLES_PER_JOURNAL = 3  # 每个期刊最多抓取的文章数量
OUTPUT_DIR = "."  # 输出目录

# 随机等待时间，模拟人类行为
def random_wait(min_seconds=1, max_seconds=3):
    time.sleep(random.uniform(min_seconds, max_seconds))

# 清理文本函数
def clean_text(text):
    if text:
        return ' '.join(text.strip().split())
    return ""

# 初始化浏览器
def init_browser():
    print("[+] 正在初始化浏览器...")
    playwright = sync_playwright().start()
    browser = playwright.chromium.launch(
        headless=False,  # 显示浏览器，方便观察
        slow_mo=50,  # 放慢操作速度
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--start-maximized"
        ]
    )
    context = browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    page = context.new_page()
    
    # 隐藏自动化特征
    page.evaluate("""
    () => {
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]});
        Object.defineProperty(navigator, 'languages', {get: () => ['zh-CN', 'zh', 'en-US', 'en']});
    }
    """)
    
    page.set_default_timeout(60000)  # 设置默认超时时间
    print("[+] 浏览器初始化完成")
    return playwright, browser, context, page

# 获取期刊列表
def get_journal_list(page):
    journals = []
    try:
        print(f"[+] 正在访问期刊列表页面: {JOURNALS_LIST_URL}")
        page.goto(JOURNALS_LIST_URL, wait_until="networkidle")
        random_wait(3, 5)  # 延长等待时间确保页面加载完成
        
        # 等待期刊列表加载完成
        print("[+] 等待期刊列表加载...")
        page.wait_for_selector('div.journal-item', timeout=30000)
        random_wait()
        
        # 获取期刊列表
        journal_items = page.query_selector_all('div.journal-item')
        print(f"[+] 找到 {len(journal_items)} 个期刊")
        
        # 提取期刊信息，限制数量
        count = 0
        for item in journal_items:
            if count >= MAX_JOURNALS:
                break
            
            try:
                name_element = item.query_selector('h2.journal-title > a')
                if name_element:
                    journal_name = clean_text(name_element.inner_text())
                    journal_url = name_element.get_attribute('href')
                    
                    if journal_url and not journal_url.startswith('http'):
                        journal_url = f"{BASE_URL}{journal_url}"
                    
                    journals.append((journal_name, journal_url))
                    print(f"[+] 已添加期刊 {count+1}/{MAX_JOURNALS}: {journal_name}")
                    count += 1
                    random_wait()
            except Exception as e:
                print(f"[-] 处理单个期刊时出错: {str(e)[:100]}")
                continue
    except Exception as e:
        print(f"[-] 获取期刊列表时出错: {str(e)[:100]}")
    
    return journals

# 获取期刊中的文章列表
def get_article_urls_from_journal(page, journal_name, journal_url):
    article_urls = []
    try:
        print(f"\n[+] 正在访问期刊页面: {journal_name}")
        page.goto(journal_url, wait_until="networkidle")
        random_wait(3, 5)
        
        # 尝试找到文章列表链接
        articles_link = None
        
        # 尝试多种可能的文章列表链接选择器
        link_selectors = [
            'a[data-title="Browse All Issues"]',
            'a[data-title="All Issues"]',
            'a[href*="issue"]',
            'a[href*="toc"]',
            'a[title="Browse All Issues"]',
            'a:has-text("Browse All Issues")',
            'a:has-text("All Issues")'
        ]
        
        for selector in link_selectors:
            try:
                link_element = page.query_selector(selector)
                if link_element:
                    articles_link = link_element.get_attribute('href')
                    print(f"[+] 找到文章列表链接: {articles_link}")
                    break
            except Exception as e:
                continue
        
        # 如果找到了文章列表链接，访问它
        if articles_link:
            if not articles_link.startswith('http'):
                articles_link = f"{BASE_URL}{articles_link}"
            
            print(f"[+] 正在访问文章列表页面: {articles_link}")
            page.goto(articles_link, wait_until="networkidle")
            random_wait(3, 5)
        
        # 获取文章链接
        article_elements = page.query_selector_all('a[href*="/doi/full/"]')
        print(f"[+] 在当前页面找到 {len(article_elements)} 篇文章链接")
        
        # 提取文章链接，限制数量
        count = 0
        for element in article_elements:
            if count >= MAX_ARTICLES_PER_JOURNAL:
                break
            
            try:
                article_url = element.get_attribute('href')
                if article_url:
                    if not article_url.startswith('http'):
                        article_url = f"{BASE_URL}{article_url}"
                    article_urls.append(article_url)
                    print(f"[+] 已添加文章链接 {count+1}/{MAX_ARTICLES_PER_JOURNAL}")
                    count += 1
            except Exception as e:
                print(f"[-] 提取文章链接时出错: {str(e)[:100]}")
                continue
    except Exception as e:
        print(f"[-] 获取期刊文章列表时出错: {str(e)[:100]}")
    
    return article_urls

# 提取文章详情信息
def extract_article_info(page, article_url, journal_name):
    article_data = {
        "作者": "",
        "标题": "",
        "期刊名称": journal_name,
        "volume": "",
        "issue": "",
        "page": "",
        "keywords": "",
        "摘要": "",
        "URL": article_url
    }
    
    try:
        print(f"[+] 正在访问文章页面: {article_url[:80]}...")
        page.goto(article_url, wait_until="networkidle")
        random_wait(3, 5)
        
        # 提取文章标题
        try:
            title_element = page.query_selector('h1.article-title')
            if title_element:
                article_data["标题"] = clean_text(title_element.inner_text())
                print(f"[+] 文章标题: {article_data['标题'][:50]}...")
        except Exception as e:
            print(f"[-] 提取标题失败: {str(e)[:50]}")
        
        # 提取作者信息（尝试多种选择器）
        try:
            authors = []
            # 尝试第一种选择器
            author_elements = page.query_selector_all('div.NLM_contrib-group > a.author')
            if not author_elements:
                # 尝试第二种选择器
                author_elements = page.query_selector_all('div.contrib-group > a.contrib-author')
            if not author_elements:
                # 尝试第三种选择器
                author_elements = page.query_selector_all('span.NLM_contrib-author')
            
            if author_elements:
                authors = [clean_text(author.inner_text()) for author in author_elements]
                article_data["作者"] = ", ".join(authors)
        except Exception as e:
            print(f"[-] 提取作者失败: {str(e)[:50]}")
        
        # 提取volume信息
        try:
            volume_element = page.query_selector('div.volume-info > span.volume')
            if volume_element:
                article_data["volume"] = clean_text(volume_element.inner_text())
        except Exception as e:
            print(f"[-] 提取volume失败: {str(e)[:50]}")
        
        # 提取issue信息
        try:
            issue_element = page.query_selector('div.volume-info > span.issue')
            if issue_element:
                article_data["issue"] = clean_text(issue_element.inner_text())
        except Exception as e:
            print(f"[-] 提取issue失败: {str(e)[:50]}")
        
        # 提取page信息
        try:
            page_element = page.query_selector('div.volume-info > span.page-range')
            if page_element:
                article_data["page"] = clean_text(page_element.inner_text())
        except Exception as e:
            print(f"[-] 提取page失败: {str(e)[:50]}")
        
        # 提取keywords信息（尝试多种选择器）
        try:
            keywords = []
            # 尝试第一种选择器
            keywords_elements = page.query_selector_all('div.article-subject-tags > a.tag')
            if not keywords_elements:
                # 尝试第二种选择器
                keywords_elements = page.query_selector_all('div.keywords-section > a.keyword')
            
            if keywords_elements:
                keywords = [clean_text(keyword.inner_text()) for keyword in keywords_elements]
                article_data["keywords"] = ", ".join(keywords)
        except Exception as e:
            print(f"[-] 提取keywords失败: {str(e)[:50]}")
        
        # 提取摘要信息（尝试多种选择器）
        try:
            # 尝试第一种选择器
            abstract_element = page.query_selector('div.abstractInFull')
            if not abstract_element:
                # 尝试第二种选择器
                abstract_element = page.query_selector('div.abstract')
            
            if abstract_element:
                article_data["摘要"] = clean_text(abstract_element.inner_text())
        except Exception as e:
            print(f"[-] 提取摘要失败: {str(e)[:50]}")
        
    except Exception as e:
        print(f"[-] 提取文章信息时出错: {str(e)[:100]}")
    
    return article_data

# 保存数据到Excel
def save_to_excel(articles_data):
    if not articles_data:
        print("[-] 没有抓取到任何文章信息，无法保存到Excel")
        return False
    
    try:
        # 创建DataFrame
        df = pd.DataFrame(articles_data)
        
        # 定义列名和顺序
        columns_order = ["作者", "标题", "期刊名称", "volume", "issue", "page", "keywords", "摘要", "URL"]
        if set(columns_order).issubset(df.columns):
            df = df[columns_order]
        
        # 生成带时间戳的文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"Taylor_Francis_Articles_{timestamp}.xlsx"
        
        # 保存到Excel文件
        df.to_excel(filename, index=False, engine='openpyxl')
        
        print(f"\n[+] 数据已成功保存到: {filename}")
        print(f"[+] 共抓取 {len(articles_data)} 篇文章信息")
        return True
    except Exception as e:
        print(f"[-] 保存数据到Excel时出错: {str(e)[:100]}")
        return False

# 主函数
def main():
    print("===== Taylor & Francis 期刊文章爬虫开始运行 ======")
    print(f"当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"配置: 抓取 {MAX_JOURNALS} 个期刊，每个期刊最多 {MAX_ARTICLES_PER_JOURNAL} 篇文章")
    
    playwright = None
    browser = None
    context = None
    all_articles_data = []
    
    try:
        # 初始化浏览器
        playwright, browser, context, page = init_browser()
        
        # 获取期刊列表
        journals = get_journal_list(page)
        print(f"[+] 共获取到 {len(journals)} 个期刊")
        
        # 遍历期刊，抓取文章信息
        for journal_name, journal_url in journals:
            # 获取当前期刊的文章链接
            article_urls = get_article_urls_from_journal(page, journal_name, journal_url)
            print(f"[+] 从期刊 '{journal_name}' 获取到 {len(article_urls)} 篇文章链接")
            
            # 创建新页面用于抓取文章详情
            article_page = context.new_page()
            try:
                # 提取每篇文章的详细信息
                for article_url in article_urls:
                    article_data = extract_article_info(article_page, article_url, journal_name)
                    if article_data["标题"]:  # 只有成功提取到标题的文章才保存
                        all_articles_data.append(article_data)
                    random_wait(2, 4)
            finally:
                article_page.close()
            
            random_wait(3, 5)  # 期刊之间的等待时间
        
        # 保存数据到Excel
        save_to_excel(all_articles_data)
        
    except Exception as e:
        print(f"[-] 程序运行时发生错误: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        # 关闭浏览器
        if context:
            context.close()
        if browser:
            browser.close()
        if playwright:
            playwright.stop()
        
        print(f"\n===== 爬虫运行结束 ======")
        print(f"当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"总共抓取到 {len(all_articles_data)} 篇有效文章")

if __name__ == "__main__":
    main()