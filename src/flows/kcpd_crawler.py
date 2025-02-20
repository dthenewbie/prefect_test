from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
from prefect import flow, task
import json
import time
import uuid
import re
from utils.text_handler import clean_content
from tasks.insert_db import save_to_caseprocessing
from utils.request_check import request_with_retry
from prefect.blocks.notifications import SlackWebhook

slack_webhook_block = SlackWebhook.load("flowcheck")

def getPageContent(soup) -> dict:
    try:
        Title = soup.select_one("tbody tr td").text.strip()
        Date = soup.select_one('div.data_midlle_news_box01.nosnippet dl dd ul').text.strip()
        Content_01 = soup.select("div.data_midlle_news_box02 ul li")
        Content_02 = soup.select("div.data_midlle_news_box02 p")
        #有兩種內文html可能
        if Content_01:
            Content = Content_01
        else:
            Content = Content_02
        content_text = "".join([ele.text for ele in Content])
        uuid_str = str(uuid.uuid3(uuid.NAMESPACE_DNS, content_text))
        # Created_time = str(datetime.now())
        data = {
            "ID": uuid_str, 
            "Title": Title, 
            "Reported_Date": Date,
            "Content": content_text, 
            "Area": None,
            "Status": 0}
    except:
        print('fail to scraped')
    return data
@task
def Scrape_page():
    result = []
    base_url = "https://kcpd-cic.kcg.gov.tw/"
    url = "https://kcpd-cic.kcg.gov.tw/News.aspx?n=F1F83458BBCAB0EB&sms=73BE5B81302C4CAD&&page=1&PageSize=100"
    response = request_with_retry(url)
    soup = BeautifulSoup(response.text, "html.parser")
    try:
        Url_list = soup.select("tbody > tr > td > p > a")
        for url_ele in Url_list: #爬蟲筆數(記得修改)
            try:
                url_tail = url_ele["href"]
                url = base_url + url_tail
                response_block = request_with_retry(url)
                soup_block = BeautifulSoup(response_block.text, "html.parser")
                data = getPageContent(soup_block)
                data["Url"] = url
                result.append(data)
                print(f"scraped: {url}")
            except:
                print("url is invailid")
    except Exception as e:
        print(e)
    
    return result
def convert_date(text):
    """
    正則找出年份、月份與日期
    """
    match = re.search(r'(\d+)-(\d+)-(\d+)', text)
    if match:
        year, month, day = map(int, match.groups())
        # 民國年轉西元年
        year += 1911
        # 回傳格式化日期
        return f"{year}-{month:02d}-{day:02d}"
    return None
@task
def data_transformation(result) -> dict:
    df = pd.DataFrame(result)
    df['Reported_Date'] = df['Reported_Date'].apply(convert_date)
    df['Content'] = df['Content'].apply(clean_content)
    result_formated = df.to_dict(orient="records")
    return result_formated

@flow(name="Kaohsiung_Police_Department_crawler")
def Kaohsiung_Police_Department_scraper_pipeline():
    try:
        # Task dependencies
        result = Scrape_page()
        result_formated = data_transformation(result)
        save_to_caseprocessing(result_formated, "Kaohsiung_Police_Department_crawler")
        slack_webhook_block.notify(f"| SUCCESS | flow 【Kaohsiung_Police_Department_crawler】 success.")
    except Exception as e:
        slack_webhook_block.notify(f"| ERROR   | flow 【Kaohsiung_Police_Department_crawler】 failed: {e}")
        print(f"| ERROR   | flow 【Kaohsiung_Police_Department_crawler】 failed: {e}")

if __name__ == "__main__":
    #Instantiate the flow
    
    # Kaohsiung_Police_Department_scraper_pipeline()


    # # temporary local server of worker
    # Kaohsiung_Police_Department_scraper_pipeline.serve(
    #     name="Kaohsiung_Police_Department_crawler",  # Deployment name. It create a temporary deployment.
    #     tags=["web crawler", "Kaohsiung_Police_Department", "case processing"],  # Filtering when searching on UI.
    #     # parameters={
    #     #     "goodbye": True
    #     # },  # Overwrite default parameters defined on hello_world_flow. Only for this deployment.
    #     # interval=60,  # Like crontab, "* * * * *"
    #     cron="*/5 * * * *",
    # )

    from prefect_github import GitHubRepository
    Kaohsiung_Police_Department_scraper_pipeline.from_source(
    source=GitHubRepository.load("antifraud"),
    entrypoint="src/flows/kcpd_crawler.py:Kaohsiung_Police_Department_scraper_pipeline",
    ).deploy(
        name="Kaohsiung_Police_Department_crawler_deployment",
        tags=["web crawler", "Kaohsiung_Police_Department", "case processing"],
        work_pool_name="antifraud",
        # job_variables=dict(pull_policy="Never"),
        # parameters=dict(name="Marvin"),
        cron="0 9 * * 3"
    )