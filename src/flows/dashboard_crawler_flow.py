from datetime import datetime, timedelta
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from tasks.insert_db import save_to_caseprocessing
import uuid
from utils.text_handler import clean_content
from utils.selenium_setting import setup_driver
from prefect import flow, task
from prefect.blocks.notifications import SlackWebhook



@task
def scrape_website(scroll_round:int) -> list:
    slack_webhook_block = SlackWebhook.load("flowcheck")
    service_url = "https://standalone-chrome-service-175804660668.asia-east1.run.app"
    url = "https://165dashboard.tw/city-case-summary"  # 目標網址
    driver = setup_driver(service_url)
    driver.get(url)

    all_data = []
    try:
        # 獲取下拉選單的所有選項
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'div[role="button"][aria-label="dropdown trigger"]'))
        ).click()
        
        options = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'p-dropdownitem li[role="option"]'))
        )
        
        time.sleep(2)
        dropdown_values = [option.text for option in options[1:]]
        for value in dropdown_values:
            print(f"正在爬取縣市：{value}")
            #選擇縣市
            WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, f'//li[@aria-label="{value}"]'))
            ).click()
            time.sleep(5)
            #爬取內容
            city_data = scrape_content(driver, value, scroll_round) 
            all_data.extend(city_data)

            # 回到首頁
            driver.get(url)
            time.sleep(5)
            WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'div[role="button"][aria-label="dropdown trigger"]'))
            ).click()
            time.sleep(5)
            
    # 處理爬取過程中的錯誤
    except Exception as e:
        slack_webhook_block.notify(f"| ERROR   | flow 【165dashboard_crawler】 error in scrape_website: {e}")
        print(f"Error: {e}")
        

    finally:
        driver.quit()

    return all_data

def scrape_content(driver, area, scroll_round:int) -> list:
    data = []
    seen_uuid = set()  # 用於記錄已處理內容的哈希值
    last_card_count = 0  # 用於追蹤區塊數量變化

    # while True:
    for _ in range(scroll_round): # ----------下拉次數------------(外部變數)
        # 抓取所有區塊
        cards = driver.find_elements(By.CSS_SELECTOR, 'div.summary-card.ng-star-inserted')
        new_cards = cards[last_card_count:]  # 只處理新加載的區塊
        
        
        for card in new_cards:
            try:
                title = card.find_element(By.CSS_SELECTOR, 'div.title').text
                content = card.find_element(By.CSS_SELECTOR, 'div.content').text.replace("\n", "")
                date = card.find_element(By.CSS_SELECTOR, 'span.summary__date').text
                date_str = date.replace("發布日期：", "")
                year, month, day = date_str.split("-")
                year = str(int(year) + 1911)
                date_str = f"{year}-{month}-{day}"
                # 計算內容的哈希值
                uuid_str = str(uuid.uuid3(uuid.NAMESPACE_DNS, content))
                
                # 檢查是否已處理過
                if uuid_str in seen_uuid:
                    continue
                # 新增到結果清單
                data.append({
                    "ID": uuid_str, #將uuid當作辨識ID
                    'Title': title,
                    'Reported_Date': date_str,
                    'Content': content,
                    'Url': "https://165dashboard.tw/city-case-summary",
                    'Area': area,  # 新增地區欄位
                    'Status': 0
                })
                # 標記為已處理
                seen_uuid.add(uuid_str)
            except Exception as e:
                print(f"Error extracting card content: {e}")
                continue
        # 更新處理過的區塊數量
        last_card_count = len(cards)  # 更新為當前已抓取的區塊總數
        if last_card_count % 100 == 0:
            print(f"-------processed data: {last_card_count}-------")
        # 滾動到頁面底部加載更多內容
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(5)  # 等待加載
        
        # 檢查是否有新內容
        retries = 3 #檢查次數
        for attempt in range(retries):
            new_card_count = len(driver.find_elements(By.CSS_SELECTOR, 'div.summary-card.ng-star-inserted'))
            if new_card_count > last_card_count:
                print(f"Newly loaded blocks:  {new_card_count - last_card_count}")  
                break
            else:
                # 如果沒有新區塊，退出迴圈
                print(f"Loading more content...{attempt + 1}/{retries}")
                time.sleep(2)
        else:
            print("No more new content to load.")
            break

    return data
@task
def data_transformation(result):
    df = pd.DataFrame(result)
    df['Content'] = df['Content'].apply(clean_content)
    result_formated = df.to_dict(orient="records")
    return result_formated
        
    
# Define task dependencies
@flow(name="165dashboard_crawler")
def dashboard_scraper_pipeline(scroll_round: int = 20):
    slack_webhook_block = SlackWebhook.load("flowcheck")
    try:
        scraped_data = scrape_website(scroll_round)
        result_formated = data_transformation(scraped_data)
        save_to_caseprocessing(result_formated, "165dashboard_crawler")
        slack_webhook_block.notify(f"| INFO    | flow 【165dashboard_crawler】 finished.")
    except Exception as e:
        slack_webhook_block.notify(f"| ERROR   | flow 【165dashboard_crawler】 error: {e}")
        # print(f"| ERROR   | flow 【165dashboard_crawler】 error: {e}")

if __name__ == "__main__":
    
    # dashboard_scraper_pipeline()

    

    # dashboard_scraper_pipeline.serve(
    #     name="165dashboard_crawler_deployment_test",  # Deployment name. It create a temporary deployment.
    #     tags=["web crawler", "165 dashboard", "case processing"],  # Filtering when searching on UI.
    #     # parameters={
    #     #     "goodbye": True
    #     # },  # Overwrite default parameters defined on hello_world_flow. Only for this deployment.
    #     # interval=60,  # Like crontab, "* * * * *"
    #     cron="*/5 * * * *",
    # )
    from prefect_github import GitHubRepository
    
    dashboard_scraper_pipeline.from_source(
    source=GitHubRepository.load("antifraud"),
    entrypoint="src/flows/dashboard_crawler_flow.py:dashboard_scraper_pipeline",
    ).deploy(
        name="165dashboard_crawler_deployment",
        tags=["web crawler", "165 dashboard", "case processing"],
        work_pool_name="antifraud",
        job_variables=dict(pull_policy="Never"),
        parameters=dict(scroll_round=int(20)),
        cron="0 10 * * *"
    )