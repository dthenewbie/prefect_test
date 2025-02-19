from prefect_github import GitHubRepository
from flows.CNA_contents_crawler import CNA_news_scraper_pipeline


CNA_news_scraper_pipeline.from_source(
    source=GitHubRepository.load("antifraud"),
    entrypoint="src/flow/CNA_contents_crawler.py:CNA_news_scraper_pipeline",
).deploy(
    name="CNA_Contents_Crawler_deployment",
    tags=["web crawler", "CNA", "case processing"],
    work_pool_name="antifraud",
    job_variables=dict(pull_policy="Never"),
    # parameters=dict(name="Marvin"),
    cron="0 12 * * *"
)