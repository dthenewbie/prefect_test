# from pyvirtualdisplay import Display
from fake_useragent import UserAgent
import selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

def setup_driver():
    """設定 Selenium 瀏覽器參數。"""
    # display = Display(visible=0, size=(1920, 1080)) 
    # display.start()
    ua = UserAgent()
    userAgent = ua.chrome
    chrome_options = Options()
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_argument("accept-language=zh-TW,zh;q=0.9,en;q=0.8")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("disable-extensions")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("window-size=1080,720")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--allow-insecure-localhost")
    # chrome_options.add_argument("--headless")
    chrome_options.add_argument("--enable-unsafe-swiftshader")
    chrome_options.add_argument('--user-agent=%s' % userAgent)
    driver = webdriver.Remote(
        command_executor=f"http://10.128.0.2:14444/wd/hub",
        options=chrome_options,
    )
    return driver