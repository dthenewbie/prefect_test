from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
from prefect import flow, task
from prefect.blocks.notifications import SlackWebhook
import time
import random
import re
import uuid
from utils.text_handler import clean_content
from tasks.insert_db import save_to_caseprocessing
from utils.request_check import request_with_retry

slack_webhook_block = SlackWebhook.load("flowcheck")

@task
def scrape_website() -> list:
        url_base = "https://www.ettoday.net/news_search/doSearch.php?keywords=%E8%A9%90%E9%A8%99&idx=1&page="
        all_data = []
        processed_data = set()
        # pages = 100
        pages = 1 # 測試限制 1 頁(記得修改)
        for page in range(1, pages+1):  
            url = f"{url_base}{page}"
            print(f"Scraping page {page}: {url}")
            try:
                response = request_with_retry(url)
                time.sleep(random.uniform(1, 2))
                soup = BeautifulSoup(response.text, "html.parser")
                page_data = scrape_page(soup)
                for data in page_data:
                    if data["Title"] not in processed_data:
                        processed_data.add(data["Title"])
                        all_data.append(data)
            except Exception as e:
                slack_webhook_block.notify(f"| ERROR   | flow 【ETtoday_crawler】 error in scrape_website[page {page}]: {e}")
                print(f"Error scraping page {page}: {e}")
            print(f"Scraped page {page} finished.")
        return all_data

def scrape_page(soup):
    data = []
    titles = soup.select('div.box_2 h2 a')
    dates = soup.select('span.date')

    for title, date in zip(titles, dates):
        create_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        pattern = re.compile(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}')
        date_text = re.search(pattern, date.text).group()
        retries = 3
        for round in range(retries):
            try:
                time.sleep(1)
                response = request_with_retry(title["href"])
                soup = BeautifulSoup(response.text, "html.parser")
                content = soup.select("div.story p")
                content_text = "".join([ele.text for ele in content[3:]])
                uuid_str = str(uuid.uuid3(uuid.NAMESPACE_DNS, content_text))
            # 處理進入各篇新聞一覽頁面連結時，爬取內容出現錯誤，重新嘗試
            except Exception as e:
                if round == retries + 1:    
                    print(f"Error fetching content: {e}")
                    content_text = None
                    uuid_str = None
                else:
                    print(f"Retrying {title['href']} (Attempt {round + 1}/{retries})")
                    time.sleep(random.randint(3))

        item = {
            "ID": uuid_str,
            "Title": title.text,
            "Reported_Date": date_text.split(" ")[0],  # 提取日期部分
            "Content": content_text,
            "Url": title["href"],
            "Area": "",
            "Status": 0  # 初始狀態為 0
        }
        data.append(item)
    return data

@task
def data_transformation(result):
    df = pd.DataFrame(result)
    df['Content'] = df['Content'].apply(clean_content)
    df['Title'] = df['Title'].apply(clean_content)
    result_formated = df.to_dict(orient="records")
    return result_formated

@flow(name="ETtoday_crawler")
def ETtoday_news_scraper_pipeline():
    try:
        scraped_data = scrape_website()
        result_formated = data_transformation(scraped_data)
        save_to_caseprocessing(result_formated, "ETtoday_crawler")
    except Exception as e:
        slack_webhook_block.notify(f"| ERROR   | flow 【ETtoday_crawler】 failed: {e}")
        print(f"| ERROR   | flow 【ETtoday_crawler】 failed: {e}")

if __name__ == "__main__":

    # ETtoday_news_scraper_pipeline()
    
    # temporary local server of worker
    ETtoday_news_scraper_pipeline.serve(
        name="ETtoday_crawler_deployment_test",  # Deployment name. It create a temporary deployment.
        tags=["web crawler", "ETtoday", "case processing"],  # Filtering when searching on UI.
        # parameters={
        #     "goodbye": True
        # },  # Overwrite default parameters defined on hello_world_flow. Only for this deployment.
        # interval=60,  # Like crontab, "* * * * *"
        cron="*/5 * * * *",
    )