from prefect_github import GitHubRepository
from flows.ettoday_crawler_flow import ETtoday_news_scraper_pipeline


ETtoday_news_scraper_pipeline.from_source(
    source=GitHubRepository.load("antifraud"),
    entrypoint="src/flows/ettoday_crawler_flow.py:ETtoday_news_scraper_pipeline",
).deploy(
    name="ETtoday_crawler_deployment",
    tags=["web crawler", "ETtoday", "case processing"],
    work_pool_name="antifraud",
    job_variables=dict(pull_policy="Never"),
    parameters=dict(pages=int(20)),
    cron="0 14 * * *",
    timezone="Asia/Taipei"
)