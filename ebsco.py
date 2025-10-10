import time
import random
import json
import os
import glob
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException

# 模拟人类行为的随机等待时间 - 优化等待时间范围
def human_like_wait(min_time=0.5, max_time=1.5):
    """模拟人类随机等待时间 - 优化为更短的等待时间"""
    wait_time = random.uniform(min_time, max_time)
    print(f"等待 {wait_time:.2f} 秒...")
    time.sleep(wait_time)

# 处理cookies弹窗
def handle_cookies(driver):
    """处理页面可能出现的cookies弹窗"""
    print("检查并处理cookies弹窗...")
    cookies_xpaths = [
        "//button[contains(@id, 'accept') or contains(@class, 'accept')]",
        "//button[text()='Accept All' or text()='接受全部']",
        "//button[contains(text(), 'cookies') and contains(text(), 'accept')]",
        "//*[@id='onetrust-accept-btn-handler']",
        "//*[@class='cookie-accept-btn']"
    ]
    
    for xpath in cookies_xpaths:
        try:
            cookies_button = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )
            print(f"找到cookies接受按钮，使用XPath: {xpath}")
            driver.execute_script("arguments[0].scrollIntoView(true);", cookies_button)
            human_like_wait()
            cookies_button.click()
            print("已点击接受cookies")
            human_like_wait()
            return True
        except (TimeoutException, NoSuchElementException, ElementClickInterceptedException):
            continue
    print("未找到或不需要处理cookies弹窗")
    return False

# 尝试点击元素，处理可能的拦截情况
def click_element(driver, element=None, by=By.XPATH, value=None, wait_time=5, force_js_click=False):
    """
    尝试点击元素，处理各种可能的点击失败情况
    force_js_click参数可强制使用JavaScript点击
    """
    try:
        if by and value:
            print(f"尝试定位元素，使用{by}: {value}")
            element = WebDriverWait(driver, wait_time).until(
                EC.presence_of_element_located((by, value))
            )
            print(f"成功定位元素")
        
        if not element:
            print("元素未找到")
            return False
        
        driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", element)
        human_like_wait()
        
        # 如果强制使用JavaScript点击或直接点击失败，使用JavaScript点击
        if force_js_click:
            print("强制使用JavaScript点击元素...")
            driver.execute_script("arguments[0].click();", element)
            print("成功使用JavaScript点击元素")
            return True
        
        try:
            # 先尝试直接点击
            element.click()
            print("成功直接点击元素")
            return True
        except ElementClickInterceptedException:
            print("直接点击失败，尝试使用JavaScript点击...")
            driver.execute_script("arguments[0].click();", element)
            print("成功使用JavaScript点击元素")
            return True
    except Exception as e:
        print(f"点击元素时出错: {str(e)}")
        return False

# 检查下载是否完成
def check_download_complete(download_dir, timeout=30):
    """检查下载是否完成，返回下载的文件路径"""
    print(f"检查下载是否完成，目录: {download_dir}")
    start_time = time.time()
    
    # 记录初始文件列表
    initial_files = set(glob.glob(os.path.join(download_dir, "*.*")))
    
    while time.time() - start_time < timeout:
        # 获取当前文件列表
        current_files = set(glob.glob(os.path.join(download_dir, "*.*")))
        
        # 找出新文件（不包括.part等临时文件）
        new_files = [f for f in current_files - initial_files if not f.endswith('.part')]
        
        if new_files:
            # 等待文件大小稳定（确保下载完成）
            file_size = os.path.getsize(new_files[0])
            time.sleep(1)
            if os.path.getsize(new_files[0]) == file_size:
                print(f"下载完成! 文件名: {os.path.basename(new_files[0])}")
                return new_files[0]
        
        time.sleep(1)
    
    print(f"下载超时({timeout}秒)，未检测到完成的文件")
    return None

# 下载EBSCO PDF文件的主函数
def download_ebsco_pdf():
    """下载EBSCO搜索结果中第一篇文章的PDF文件"""
    driver = None
    try:
        print("开始执行EBSCO PDF下载爬虫...")
        
        # 创建Edge浏览器选项
        options = Options()
        
        # 设置下载目录为绝对路径，确保正确下载
        download_dir = os.path.abspath(os.path.join(os.getcwd(), "downloads"))
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
            print(f"创建下载文件夹: {download_dir}")
        print(f"PDF文件将下载到: {download_dir}")
        
        # 优化Edge浏览器的下载配置
        prefs = {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True,
            "safebrowsing.enabled": False,  # 禁用安全浏览检查
            "download_restrictions": 0,  # 允许所有下载
            "profile.default_content_setting_values.automatic_downloads": 1,  # 允许自动下载
            "profile.default_content_settings.popups": 0  # 禁止弹出窗口
        }
        
        # 添加实验性选项
        options.add_experimental_option("prefs", prefs)
        # 禁用自动关闭下载弹窗
        options.add_argument("--disable-popup-blocking")
        
        print("正在初始化Edge浏览器...")
        driver = webdriver.Edge(options=options)
        driver.set_window_size(1200, 800)
        
        # 访问目标网页
        url = "https://research.ebsco.com/c/vlgzj5/search/results?q=JN%20%22Accounting%20Review%22%20AND%20DT%2020250901%20NOT%20PM%20AOP&autocorrect=y&db=aph%2Cbth&expanders=concept&facetFilter=databases%3AYnRo&limiters=FT%3AY&searchMode=boolean&searchSegment=all-results"
        print(f"正在访问网页: `{url}` ")
        driver.get(url)
        
        # 等待页面加载
        human_like_wait(1, 2)
        
        # 处理cookies弹窗
        handle_cookies(driver)
        
        print("等待搜索结果加载...")
        human_like_wait(1, 2)
        
        print("点击第一篇文章的标题...")
        first_article_title = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//a[contains(@id, '-bth')]"))
        )
        
        if not click_element(driver, first_article_title):
            print("尝试使用备用方式点击第一篇文章标题...")
            article_url = first_article_title.get_attribute('href')
            if article_url:
                print(f"直接访问文章链接: {article_url}")
                driver.get(article_url)
            else:
                raise Exception("无法获取文章链接")
        
        human_like_wait(1, 2)  # 等待文章详情页加载
        
        print("点击第一个下载按钮...")
        download_button1_xpath = "//*[@id='details-page']/div[1]/div/section/div[3]/div/div[2]/div/button"
        if not click_element(driver, None, By.XPATH, download_button1_xpath):
            alt_download_xpaths = [
                "//button[contains(@class, 'download')]",
                "//button[contains(text(), 'Download') or contains(text(), '下载')]",
                "//*[contains(@id, 'download') and contains(@type, 'button')]",
                "//*[@class='full-text-options']//button",
                "//button[contains(@aria-label, 'Download')]"
            ]
            found = False
            for xpath in alt_download_xpaths:
                if click_element(driver, None, By.XPATH, xpath):
                    found = True
                    break
            if not found:
                raise Exception("未找到下载按钮")
        
        print("等待下载弹窗出现...")
        human_like_wait(1, 2)  # 等待弹窗加载
        
        print("点击第二个确认下载按钮...")
        # 使用用户提供的XPath，并确保使用JavaScript点击
        download_button2_xpath = "/html/body/div[17]/div/div/div[3]/button[2]"
        if not click_element(driver, None, By.XPATH, download_button2_xpath, wait_time=8, force_js_click=True):
            # 尝试其他可能的确认下载按钮XPath
            alt_confirm_xpaths = [
                "//button[contains(@class, 'primary') and contains(@class, 'button')]",  # 用户提到的有效选择器
                "/html/body/div[16]/div/div/div[3]/button[2]",
                "/html/body/div[18]/div/div/div[3]/button[2]",
                "//div[contains(@class, 'modal')]//button[contains(@class, 'primary')]",
                "//button[contains(text(), 'PDF Full Text')]",
                "//button[contains(text(), 'Download PDF')]"
            ]
            found = False
            for xpath in alt_confirm_xpaths:
                print(f"尝试使用备选XPath: {xpath}")
                if click_element(driver, None, By.XPATH, xpath, wait_time=5, force_js_click=True):
                    found = True
                    break
            
            if not found:
                print("所有确认下载按钮选择器都失败，尝试查找直接下载链接...")
                try:
                    direct_download_link = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, "//a[contains(@href, 'pdf') and contains(@class, 'download')]"))
                    )
                    driver.execute_script("arguments[0].click();", direct_download_link)
                    found = True
                    print("成功通过直接下载链接开始下载")
                except:
                    print("无法找到直接下载链接")
            
            if not found:
                print("无法找到确认下载按钮，以下是页面的部分源代码：")
                print(driver.page_source[:1000])
                raise Exception("未找到确认下载按钮")
        
        print("开始下载PDF文件...")
        # 使用下载检查函数确保文件正确下载完成
        downloaded_file = check_download_complete(download_dir)
        
        if downloaded_file:
            print(f"PDF文件下载成功! 已保存至: {downloaded_file}")
        else:
            print("PDF文件下载失败或未完成")
        
    except Exception as e:
        print(f"发生错误: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        if driver:
            print("正在关闭浏览器...")
            driver.quit()
        print("爬虫执行完毕")

# 主程序入口
if __name__ == "__main__":
    download_ebsco_pdf()