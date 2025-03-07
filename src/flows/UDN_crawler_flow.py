from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
from prefect import flow, task
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
import time
from tasks.insert_db import save_to_caseprocessing
import uuid
from utils.text_handler import clean_content
from utils.request_check import request_with_retry
from utils.selenium_setting import setup_driver
from prefect.blocks.notifications import SlackWebhook


def scrape_news_details(detail_url) -> str:
    """進入新聞詳細頁，帶重試機制爬取內文內容。"""
    try:
        response = request_with_retry(detail_url)
        soup = BeautifulSoup(response.text, "html.parser")
        paragraphs = soup.select("div.article-content__paragraph p")
        if not paragraphs:
            raise ValueError("No content found on the page.")

        content = "\n".join([p.text for p in paragraphs[:-2]])
    except Exception as e:
        print(f"Error while scraping {detail_url}: {e}")
        content = f"Error: {str(e)}"
    return content

@task()
def scrape_main_page(scroll_round: int = 20):
    """爬取主頁內容，並進一步進入每個新聞的詳細頁。"""
    url = "https://udn.com/search/tagging/2/%E8%A9%90%E9%A8%99%E9%9B%86%E5%9C%98"
    driver = setup_driver()


    processed_urls = set()
    all_results = []
    error_log = []

    try:
        for attempt in range(3):
            try:
                driver.get(url)
                break
            except TimeoutException:
                print(f"Timeout occurred for URL: {url}. Retrying... (Attempt {attempt + 1}/3)")
            except Exception as e:
                print(f"Error occurred for URL: {url}. Error: {e}. Retrying... (Attempt {attempt + 1}/3)")
        # while True:
        for _ in range(scroll_round): # ----------下拉次數------------(外部參數)
            news_blocks = driver.find_elements(By.CSS_SELECTOR, "div.story-list__news")
            if not news_blocks:
                print("No news blocks found.")
                break

            for block in news_blocks:
                try:
                    title_element = block.find_element(By.CSS_SELECTOR, "div.story-list__text h2 a")
                    date_element = block.find_element(By.CSS_SELECTOR, "div.story-list__info time.story-list__time")
                    link_element = block.find_element(By.CSS_SELECTOR, "div.story-list__text h2 a[href]")

                    title = title_element.text
                    date = date_element.text
                    detail_url = link_element.get_attribute("href")

                    if detail_url in processed_urls:
                        continue

                    processed_urls.add(detail_url)

                    # 進入新聞詳細頁，爬取內文
                    content = scrape_news_details(detail_url)
                    news_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, content))

                    if "Failed" in content or "Error" in content:
                        error_log.append({"URL": detail_url, "Error": content})
                    else:
                        all_results.append({
                            "ID": news_id,
                            "Title": title,
                            "Reported_Date": date.split(" ")[0],
                            "Content": content,
                            "Url": detail_url,
                            "Area": None,
                            "Status": 0
                        })
                        print(f"Scraped: {title} ({date})")
                except NoSuchElementException:
                    continue
                except Exception as e:
                    print(f"Error while processing news block: {e}")

            # 模擬滾動加載新內容
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            print("Scrolling to load more content...")

            # 檢查是否有新內容
            new_news_blocks = driver.find_elements(By.CSS_SELECTOR, "div.story-list__news")
            if len(new_news_blocks) <= len(news_blocks):
                print("No more new content. Stopping the scrape.")
                break

    finally:
        driver.quit()

    return all_results
@task
def data_transformation(result):
    df = pd.DataFrame(result)
    # 將字串轉換為日期格式，再格式化成目標格式
    df['Content'] = df['Content'].apply(clean_content)
    result_formated = df.to_dict(orient="records")
    return result_formated

@flow(name="UDN_news_scraper_pipeline")
def UDN_news_scraper_pipeline(scroll_round: int = 20):
    slack_webhook_block = SlackWebhook.load("flowcheck")
    try:
        # Define task dependencies
        scraped_data = scrape_main_page(scroll_round)
        result_formated = data_transformation(scraped_data)
        save_to_caseprocessing(result_formated, "UDN_news_scraper_pipeline")
    except Exception as e:
        slack_webhook_block.notify(f"| ERROR   | flow 【UDN_news_scraper_pipeline】 failed: {e}")
        print(f"| ERROR   | flow 【UDN_news_scraper_pipeline】 failed: {e}")

if __name__ == "__main__":
    # # Instantiate the flow

    # UDN_news_scraper_pipeline()

    # # temporary local server of worker
    # UDN_news_scraper_pipeline.serve(
    #     name="UDN_news_crawler",  # Deployment name. It create a temporary deployment.
    #     tags=["web crawler", "UDN", "case processing"],  # Filtering when searching on UI.
    #     # parameters={
    #     #     "goodbye": True
    #     # },  # Overwrite default parameters defined on hello_world_flow. Only for this deployment.
    #     # interval=60,  # Like crontab, "* * * * *"
    #     cron="*/5 * * * *",
    # )

    from prefect_github import GitHubRepository
    UDN_news_scraper_pipeline.from_source(
    source=GitHubRepository.load("antifrauddocker"),
    entrypoint="src/flows/UDN_crawler_flow.py:UDN_news_scraper_pipeline",
    ).deploy(
        name="UDN_news_crawler_deployment",
        tags=["web crawler", "UDN", "case processing"],
        work_pool_name="antifrauddocker",
        parameters=dict(scroll_round= int(20)),
        cron="0 15 * * *"
    )
