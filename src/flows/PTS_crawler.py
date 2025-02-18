from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import pandas as pd
from prefect import flow, task
import time
import random
import uuid
from tasks.insert_db import save_to_caseprocessing
from utils.text_handler import clean_content
from utils.request_check import request_with_retry
from prefect.blocks.notifications import SlackWebhook

slack_webhook_block = SlackWebhook.load("flowcheck")

@task
def scrape_website() -> list:
    url_start = "https://news.pts.org.tw/tag/128?page="
    url_tail = "&type=new"
    seen_ID = set()
    all_data = []
    pagenum = 1  # 測試限制爬取 i 頁 (記得修改)<共100頁>

    for page in range(1, pagenum + 1):
        url = url_start + str(page) + url_tail
        print(f"Scraping page {page}: {url}")
        response = request_with_retry(url)
        soup = BeautifulSoup(response.text, "html.parser")
        # 爬取當前頁面資料
        page_data = scrape_page(soup)
        for data in page_data:
            if data["ID"] not in seen_ID:
                seen_ID.add(data["ID"])
                all_data.append(data)
        print(f"{len(all_data)} data scraped.")
    return all_data

def scrape_page(soup):
    data = []
    # 爬取所有目標資料
    titles = soup.select('div.pt-2.pt-md-0 h2 a')[:5]
    dates = soup.select('div.news-info time')[:5]
    for title, date in zip(titles, dates):
        create_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 進入文章內容
        response = request_with_retry(title["href"])
        soup = BeautifulSoup(response.text, "html.parser")
        try:
            content = soup.select_one('div.post-article.text-align-left').text.replace("\n", "")
            uuid_str = str(uuid.uuid3(uuid.NAMESPACE_DNS, content))
        except Exception as e:
            print(f"Error fetching content: {e}")
            content = None
            uuid_str = None
        item = {
            "ID": uuid_str,
            "Title": title.text.replace("\n", ""),
            "Reported_Date": date.text.split(" ")[0],
            "Content": content,
            "Url": title["href"],
            "Area": None,
            "Status": 0
        }
        data.append(item)
        time.sleep(random.uniform(1, 2))
    return data

@task
def data_transformation(result):
    df = pd.DataFrame(result)
    # 將字串轉換為日期格式，再格式化成目標格式
    df['Content'] = df['Content'].apply(clean_content)
    result_formated = df.to_dict(orient="records")
    return result_formated

@flow(name="PTS_crawler")
def PTS_news_scraper_pipeline():
    try:
        # Task dependencies
        scraped_data = scrape_website()
        result_formated = data_transformation(scraped_data)
        save_to_caseprocessing(result_formated, "PTS_crawler")
    except Exception as e:
        slack_webhook_block.notify(f"| ERROR   | flow 【PTS_crawler】 failed: {e}")
        print(f"| ERROR   | flow 【PTS_crawler】 failed: {e}")

if __name__ == "__main__":
    # # Instantiate the flow
    # PTS_news_scraper_pipeline()


    # temporary local server of worker
    PTS_news_scraper_pipeline.serve(
        name="pts_news_crawler",  # Deployment name. It create a temporary deployment.
        tags=["web crawler", "PTS", "case processing"],  # Filtering when searching on UI.
        # parameters={
        #     "goodbye": True
        # },  # Overwrite default parameters defined on hello_world_flow. Only for this deployment.
        # interval=60,  # Like crontab, "* * * * *"
        cron="*/5 * * * *",
    )