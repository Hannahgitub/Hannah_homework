import random
import time  # 修复：添加缺失的time模块导入
import json  # 修复：添加缺失的json模块导入
import requests
from datetime import datetime
from playwright.sync_api import sync_playwright

# 全局配置
BASE_URL = "https://www.tandfonline.com"
JOURNALS_LIST_URL = f"{BASE_URL}/action/showPublications?pubType=journal"
MAX_JOURNALS = 2  # 限制抓取的期刊数量，先从少量开始测试
MAX_ARTICLES_PER_JOURNAL = 3  # 每个期刊最多抓取的文章数量
OUTPUT_DIR = "."  # 输出目录
CROSSREF_API_URL = "https://api.crossref.org/works/{doi}"  # Crossref API基础URL

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

# 从URL中提取DOI
def extract_doi_from_url(url):
    try:
        # 从URL中提取DOI部分
        if '/doi/' in url:
            parts = url.split('/doi/')
            if len(parts) > 1:
                doi_part = parts[1].split('/')[0] if '/' in parts[1] else parts[1]
                # 移除可能的查询参数
                if '?' in doi_part:
                    doi_part = doi_part.split('?')[0]
                return doi_part
        return None
    except Exception as e:
        print(f"[-] 从URL提取DOI失败: {str(e)}")
        return None

# 通过Crossref API获取文章信息
def get_article_info_via_crossref(doi):
    """
    使用Crossref API获取文章信息
    """
    if not doi:
        print("[-] 无效的DOI")
        return None
    
    try:
        # 构建完整的API URL
        api_url = CROSSREF_API_URL.format(doi=doi)
        print(f"[+] 正在通过Crossref API获取信息: {api_url}")
        
        # 设置请求头
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        # 发送GET请求
        response = requests.get(api_url, headers=headers, timeout=10)
        response.raise_for_status()  # 如果状态码不是200，抛出异常
        
        # 解析JSON响应
        data = response.json()
        
        if "message" in data:
            message = data["message"]
            article_data = {
                "作者": "",
                "标题": "",
                "期刊名称": "",
                "volume": "",
                "issue": "",
                "page": "",
                "keywords": "",
                "摘要": "",
                "年份": "",
                "URL": f"https://doi.org/{doi}"
            }
            
            # 提取作者信息
            if "author" in message:
                authors = []
                for author in message["author"]:
                    if "given" in author and "family" in author:
                        authors.append(f"{author['given']} {author['family']}")
                    elif "name" in author:
                        authors.append(author["name"])
                article_data["作者"] = ", ".join(authors)
            
            # 提取标题信息
            if "title" in message and message["title"]:
                article_data["标题"] = clean_text(message["title"][0])
            
            # 提取期刊名称
            if "container-title" in message and message["container-title"]:
                article_data["期刊名称"] = clean_text(message["container-title"][0])
            
            # 提取volume信息
            if "volume" in message:
                article_data["volume"] = clean_text(str(message["volume"]))
            
            # 提取issue信息
            if "issue" in message:
                article_data["issue"] = clean_text(str(message["issue"]))
            
            # 提取page信息
            if "page" in message:
                article_data["page"] = clean_text(str(message["page"]))
            
            # 提取摘要信息
            if "abstract" in message:
                article_data["摘要"] = clean_text(message["abstract"])
            
            # 提取年份信息
            if "published-print" in message and "date-parts" in message["published-print"]:
                date_parts = message["published-print"]["date-parts"][0]
                if date_parts and len(date_parts) > 0:
                    article_data["年份"] = str(date_parts[0])
            elif "published-online" in message and "date-parts" in message["published-online"]:
                date_parts = message["published-online"]["date-parts"][0]
                if date_parts and len(date_parts) > 0:
                    article_data["年份"] = str(date_parts[0])
            
            return article_data
        else:
            print("[-] Crossref API响应中没有找到文章信息")
            return None
    except Exception as e:
        print(f"[-] 通过Crossref API获取信息时出错: {str(e)}")
        return None

# 提取文章信息
def extract_article_info(page, article_url):
    article_data = {
        "作者": "",
        "标题": "",
        "期刊名称": "",
        "volume": "",
        "issue": "",
        "page": "",
        "keywords": "",
        "摘要": "",
        "年份": "",
        "URL": article_url
    }
    
    try:
        print(f"[+] 正在访问文章页面: {article_url[:80]}...")
        page.goto(article_url, wait_until="networkidle")
        random_wait(3, 5)
        
        # 提取文章标题
        try:
            title_element = page.query_selector('h1.article-title')
            if not title_element:
                title_element = page.query_selector('h1')
            if title_element:
                article_data["标题"] = clean_text(title_element.inner_text())
                print(f"[+] 文章标题: {article_data['标题'][:50]}...")
        except Exception as e:
            print(f"[-] 提取标题失败: {str(e)[:50]}")
        
        # 提取作者信息
        try:
            authors = []
            # 尝试多种选择器
            author_elements = page.query_selector_all('div.NLM_contrib-group > a.author')
            if not author_elements:
                author_elements = page.query_selector_all('div.contrib-group > a.contrib-author')
            if not author_elements:
                author_elements = page.query_selector_all('span.NLM_contrib-author')
            if not author_elements:
                author_elements = page.query_selector_all('div.author-info > a')
            
            # 添加用户提供的XPath选择器作为备选方案
            if not author_elements:
                try:
                    # 使用XPath选择器
                    author_xpath = "//*[@id='fa57727f-b942-4eb8-9ed2-ecfe11ac03f5']/div/div/div[3]/div/div/span/span[1]/div/a"
                    author_elements = page.locator(author_xpath).element_handles()
                    # 可能需要寻找更多的作者元素，因为上面的XPath可能只指向第一个作者
                    if not author_elements:
                        # 尝试寻找所有可能的作者元素
                        author_elements = page.query_selector_all('a[data-test="author-name"]')
                except:
                    pass
            
            if author_elements:
                authors = [clean_text(author.inner_text()) for author in author_elements]
                article_data["作者"] = ", ".join(authors)
            else:
                # 尝试直接从元数据中提取
                meta_authors = page.query_selector("meta[name='citation_author']")
                if meta_authors:
                    article_data["作者"] = clean_text(meta_authors.get_attribute('content'))
        except Exception as e:
            print(f"[-] 提取作者失败: {str(e)[:50]}")
        
        # 提取期刊名称
        try:
            journal_element = page.query_selector('a.journal-title')
            if not journal_element:
                journal_element = page.query_selector('div.journal-header > h2')
            
            # 添加用户提供的包含期刊名称的XPath作为备选方案
            if not journal_element:
                try:
                    # 使用XPath选择器获取包含期刊名称的元素
                    journal_info_xpath = "//*[@id='7730bfe1-9fca-4cf4-a6d6-2a0148105437']/div/div/div/div[2]/div[1]"
                    journal_info_element = page.locator(journal_info_xpath).element_handle()
                    if journal_info_element:
                        journal_info_text = clean_text(journal_info_element.inner_text())
                        # 从期刊信息中提取期刊名称（通常在开头部分）
                        if journal_info_text:
                            # 假设期刊名称在逗号前或特定格式中
                            if ',' in journal_info_text:
                                article_data["期刊名称"] = clean_text(journal_info_text.split(',')[0])
                            else:
                                article_data["期刊名称"] = journal_info_text
                except:
                    pass
            
            if not article_data["期刊名称"] and journal_element:
                article_data["期刊名称"] = clean_text(journal_element.inner_text())
            
            # 尝试从元数据中提取
            if not article_data["期刊名称"]:
                meta_journal = page.query_selector("meta[name='citation_journal_title']")
                if meta_journal:
                    article_data["期刊名称"] = clean_text(meta_journal.get_attribute('content'))
        except Exception as e:
            print(f"[-] 提取期刊名称失败: {str(e)[:50]}")
        
        # 提取volume信息
        try:
            volume_element = page.query_selector('div.volume-info > span.volume')
            
            # 添加用户提供的包含volume的XPath作为备选方案
            if not volume_element:
                try:
                    # 使用XPath选择器获取包含volume的元素
                    journal_info_xpath = "//*[@id='7730bfe1-9fca-4cf4-a6d6-2a0148105437']/div/div/div/div[2]/div[1]"
                    journal_info_element = page.locator(journal_info_xpath).element_handle()
                    if journal_info_element:
                        journal_info_text = clean_text(journal_info_element.inner_text())
                        # 从期刊信息中提取volume
                        import re
                        volume_match = re.search(r'Volume\s+(\d+)', journal_info_text, re.IGNORECASE)
                        if volume_match:
                            article_data["volume"] = volume_match.group(1)
                except:
                    pass
            
            if not article_data["volume"]:
                # 尝试从元数据中提取
                meta_volume = page.query_selector("meta[name='citation_volume']")
                if meta_volume:
                    article_data["volume"] = clean_text(meta_volume.get_attribute('content'))
            
            if not article_data["volume"] and volume_element:
                article_data["volume"] = clean_text(volume_element.inner_text())
        except Exception as e:
            print(f"[-] 提取volume失败: {str(e)[:50]}")
        
        # 提取issue信息
        try:
            issue_element = page.query_selector('div.volume-info > span.issue')
            if not issue_element:
                # 尝试从元数据中提取
                meta_issue = page.query_selector("meta[name='citation_issue']")
                if meta_issue:
                    article_data["issue"] = clean_text(meta_issue.get_attribute('content'))
                else:
                    # 尝试从期刊信息中提取issue
                    try:
                        journal_info_xpath = "//*[@id='7730bfe1-9fca-4cf4-a6d6-2a0148105437']/div/div/div/div[2]/div[1]"
                        journal_info_element = page.locator(journal_info_xpath).element_handle()
                        if journal_info_element:
                            journal_info_text = clean_text(journal_info_element.inner_text())
                            import re
                            issue_match = re.search(r'Issue\s+(\d+)', journal_info_text, re.IGNORECASE)
                            if issue_match:
                                article_data["issue"] = issue_match.group(1)
                    except:
                        pass
            else:
                article_data["issue"] = clean_text(issue_element.inner_text())
        except Exception as e:
            print(f"[-] 提取issue失败: {str(e)[:50]}")
        
        # 提取page信息
        try:
            page_element = page.query_selector('div.volume-info > span.page-range')
            
            # 添加用户提供的page的XPath作为备选方案
            if not page_element:
                try:
                    page_xpath = "//*[@id='5a6ad2bb-9f9c-47b0-9143-56cc8552d601']/div/div/div/span[1]"
                    page_element = page.locator(page_xpath).element_handle()
                except:
                    pass
            
            if page_element:
                article_data["page"] = clean_text(page_element.inner_text())
            else:
                # 尝试从元数据中提取
                meta_firstpage = page.query_selector("meta[name='citation_firstpage']")
                meta_lastpage = page.query_selector("meta[name='citation_lastpage']")
                if meta_firstpage:
                    page_info = clean_text(meta_firstpage.get_attribute('content'))
                    if meta_lastpage:
                        page_info += "-" + clean_text(meta_lastpage.get_attribute('content'))
                    article_data["page"] = page_info
        except Exception as e:
            print(f"[-] 提取page失败: {str(e)[:50]}")
        
        # 提取keywords信息
        try:
            keywords = []
            # 尝试多种选择器
            keywords_elements = page.query_selector_all('div.article-subject-tags > a.tag')
            if not keywords_elements:
                keywords_elements = page.query_selector_all('div.keywords-section > a.keyword')
            if not keywords_elements:
                keywords_elements = page.query_selector_all('span.keyword')
            
            # 添加用户提供的keywords的XPath作为备选方案
            if not keywords_elements:
                try:
                    keywords_xpath = "//*[@id='mainTabPanel']/article/div[1]/div[2]/div/div/ul"
                    keywords_list = page.locator(keywords_xpath).element_handle()
                    if keywords_list:
                        # 获取ul下的所有li元素
                        keywords_elements = keywords_list.query_selector_all('li')
                except:
                    pass
            
            if keywords_elements:
                keywords = [clean_text(keyword.inner_text()) for keyword in keywords_elements]
                article_data["keywords"] = ", ".join(keywords)
        except Exception as e:
            print(f"[-] 提取keywords失败: {str(e)[:50]}")
        
        # 提取摘要信息
        try:
            # 尝试多种选择器
            abstract_element = page.query_selector('div.abstractInFull')
            if not abstract_element:
                abstract_element = page.query_selector('div.abstract')
            if not abstract_element:
                abstract_element = page.query_selector('div.abstractSection')
            
            # 添加用户提供的abstract的XPath作为备选方案
            if not abstract_element:
                try:
                    abstract_xpath = "//*[@id='abstractId1']"
                    abstract_element = page.locator(abstract_xpath).element_handle()
                except:
                    pass
            
            if abstract_element:
                article_data["摘要"] = clean_text(abstract_element.inner_text())
        except Exception as e:
            print(f"[-] 提取摘要失败: {str(e)[:50]}")
        
        # 提取年份信息
        try:
            # 尝试从用户提供的包含年份的XPath中提取
            try:
                journal_info_xpath = "//*[@id='7730bfe1-9fca-4cf4-a6d6-2a0148105437']/div/div/div/div[2]/div[1]"
                journal_info_element = page.locator(journal_info_xpath).element_handle()
                if journal_info_element:
                    journal_info_text = clean_text(journal_info_element.inner_text())
                    import re
                    # 匹配年份（4位数字）
                    year_match = re.search(r'(\b\d{4}\b)', journal_info_text)
                    if year_match:
                        article_data["年份"] = year_match.group(1)
            except:
                pass
            
            # 如果上面的方法失败，尝试从元数据中提取
            if not article_data["年份"]:
                meta_date = page.query_selector("meta[name='citation_publication_date']")
                if meta_date:
                    date_str = clean_text(meta_date.get_attribute('content'))
                    # 从日期字符串中提取年份
                    if date_str:
                        # 处理各种日期格式 YYYY, YYYY/MM/DD, YYYY-MM-DD 等
                        if len(date_str) >= 4 and date_str[:4].isdigit():
                            article_data["年份"] = date_str[:4]
        except Exception as e:
            print(f"[-] 提取年份失败: {str(e)[:50]}")
        
    except Exception as e:
        print(f"[-] 提取文章信息时出错: {str(e)[:100]}")
        import traceback
        traceback.print_exc()
    
    return article_data

# CrossAPI迭代方法 - 用于迭代处理多个URL
def crossapi_iterate_urls(page, urls):
    """
    使用迭代器模式处理多个URL，优先尝试通过Crossref API获取信息
    
    Args:
        page: Playwright页面对象
        urls: 待处理的URL列表
        
    Yields:
        每个URL对应的文章信息字典
    """
    for url in urls:
        try:
            # 首先尝试通过Crossref API获取信息
            doi = extract_doi_from_url(url)
            if doi:
                print(f"[+] 找到DOI: {doi}")
                crossref_data = get_article_info_via_crossref(doi)
                if crossref_data:
                    yield crossref_data
                    random_wait(2, 4)  # 每个URL之间的等待时间
                    continue
                else:
                    print("[-] Crossref API未能获取到信息，尝试通过网页抓取")
            
            # 如果Crossref API失败，回退到网页抓取
            article_data = extract_article_info(page, url)
            yield article_data
            random_wait(2, 4)  # 每个URL之间的等待时间
        except Exception as e:
            print(f"[-] 处理URL {url} 时出错: {str(e)}")
            # 即使出错也继续处理下一个URL
            continue

# 保存数据到JSON
def save_to_json(articles_data):
    if not articles_data:
        print("[-] 没有抓取到任何文章信息，无法保存到JSON")
        return False
    
    try:
        # 生成带时间戳的文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"Taylor_Francis_Articles_{timestamp}.json"
        
        # 保存到JSON文件
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(articles_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n[+] 数据已成功保存到: {filename}")
        print(f"[+] 共抓取 {len(articles_data)} 篇文章信息")
        return True
    except Exception as e:
        print(f"[-] 保存数据到JSON时出错: {str(e)[:100]}")
        return False

# 主函数
def main():
    print("===== Taylor & Francis 期刊文章爬虫开始运行 ======")
    print(f"当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 测试URL列表
    urls_to_process = [
        "https://www.tandfonline.com/doi/full/10.1080/07421222.2025.2520170"
        # 可以在这里添加更多URL进行批量处理
    ]
    
    playwright = None
    browser = None
    context = None
    all_articles_data = []
    
    try:
        # 初始化浏览器
        playwright, browser, context, page = init_browser()
        
        # 使用CrossAPI迭代方法处理URL列表
        print(f"[+] 开始使用CrossAPI方法迭代处理 {len(urls_to_process)} 个URL")
        for article_data in crossapi_iterate_urls(page, urls_to_process):
            all_articles_data.append(article_data)
            
            # 打印提取的信息
            print("\n[+] 提取的文章信息:")
            for key, value in article_data.items():
                print(f"{key}: {value or '未找到'}")
        
        # 保存数据到JSON
        save_to_json(all_articles_data)
        
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
    
    print("\n===== 爬虫运行结束 ======")

if __name__ == "__main__":
    main()