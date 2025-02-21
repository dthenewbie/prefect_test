from prefect_github import GitHubRepository
from flows.UDN_crawler_flow import UDN_news_scraper_pipeline


UDN_news_scraper_pipeline.from_source(
    source=GitHubRepository.load("antifraud"),
    entrypoint="src/flows/UDN_crawler_flow.py:UDN_news_scraper_pipeline",
).deploy(
    name="UDN_news_crawler_deployment",
    tags=["web crawler", "UDN", "case processing"],
    work_pool_name="antifraud",
    job_variables=dict(pull_policy="Never"),
    parameters=dict(scroll_round= int(20)),
    cron="0 15 * * *",
    timezone="Asia/Taipei"
)