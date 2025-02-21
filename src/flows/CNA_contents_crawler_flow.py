from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
from prefect import task, flow
from prefect.blocks.notifications import SlackWebhook
import uuid
from tasks.insert_db import save_to_caseprocessing
from utils.text_handler import clean_content
from utils.request_check import request_with_retry






def parse_article_links(html) -> list:
    soup = BeautifulSoup(html.text, 'html.parser')
    articles = soup.select("#jsMainList a")
    return ["https://www.cna.com.tw" + a['href'] for a in articles]

def fetch_article_content(url) -> dict:
    response = request_with_retry(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    try:
        title = soup.title.text.strip()
        date_tag = soup.select_one(".updatetime")
        date = date_tag.text.strip() if date_tag else "無時間"
        inner_text = soup.find("div", {"class": "paragraph"})
        if not inner_text:
            content_text = None
        paragraphs = inner_text.find_all('p')
        content_text = " ".join(p.text for p in paragraphs)
        uuid_str = str(uuid.uuid3(uuid.NAMESPACE_DNS, content_text))
        article = {"ID":uuid_str, 
                "Title": title, 
                "Reported_Date": date.split(" ")[0], 
                "Content": content_text, 
                "Url": url,
                "Area":None}
    except Exception as e:
        article = None
        print(e)

    return article
@task
def scrape_page():
    search_url = "https://www.cna.com.tw/search/hysearchws.aspx?q=%E8%A9%90%E9%A8%99"
    html = request_with_retry(search_url)
    if not html:
        return

    article_links = parse_article_links(html)
    news = []

    for link in article_links:
        article = fetch_article_content(link)
        if article:
            print(f"scraped: {article['Title']}")
            news.append(article)

    return news
@task
def data_transformation(result):
    df = pd.DataFrame(result)
    df['Content'] = df['Content'].apply(clean_content)
    result_formated = df.to_dict(orient="records")
    return result_formated

@flow(name="CNA_contents_crawler")
def CNA_news_scraper_pipeline():
    slack_webhook_block = SlackWebhook.load("flowcheck")
    try:
        # Task dependencies
        scraped_data = scrape_page()
        result_formated = data_transformation(scraped_data)
        save_to_caseprocessing(result_formated, "CNA_Contents_Crawler")
        slack_webhook_block.notify(f"| INFO    | flow 【CNA_contents_crawler】 finished")
    except Exception as e:
        slack_webhook_block.notify(f"| ERROR   | flow 【CNA_contents_crawler】 error: {e}")
        print(f"| ERROR   | flow 【CNA_contents_crawler】 error: {e}")

if __name__ == "__main__":
    # Instantiate the flow
    
    # CNA_news_scraper_pipeline()

    # temporary local server of worker
    # CNA_news_scraper_pipeline.serve(
    #     name="CNA_Contents_Crawler_deployment_test",  # Deployment name. It create a temporary deployment.
    #     tags=["web crawler", "CNA", "case processing"],  # Filtering when searching on UI.
    #     # parameters={
    #     #     "goodbye": True
    #     # },  # Overwrite default parameters defined on hello_world_flow. Only for this deployment.
    #     # interval=60,  # Like crontab, "* * * * *"
    #     cron="*/5 * * * *",
    # )
    from prefect_github import GitHubRepository
    
    CNA_news_scraper_pipeline.from_source(
    source=GitHubRepository.load("antifraud"),
    entrypoint="src/flows/CNA_contents_crawler_flow.py:CNA_news_scraper_pipeline",
    ).deploy(
        name="CNA_Contents_Crawler_deployment",
        tags=["web crawler", "CNA", "case processing"],
        work_pool_name="antifraud",
        job_variables=dict(pull_policy="Never"),
        # parameters=dict(name="Marvin"),
        cron="0 12 * * *"
    )