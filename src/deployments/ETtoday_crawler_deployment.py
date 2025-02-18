from prefect_github import GitHubRepository
from flows.ettoday_crawler import ETtoday_news_scraper_pipeline


ETtoday_news_scraper_pipeline.from_source(
    source=GitHubRepository.load("github-prefect-demo"),
    entrypoint="src/flow/ettoday_crawler.py:ETtoday_news_scraper_pipeline",
).deploy(
    name="ETtoday_crawler_deployment",
    tags=["web crawler", "ETtoday", "case processing"],
    work_pool_name="test-docker",
    job_variables=dict(pull_policy="Never"),
    # parameters=dict(name="Marvin"),
    cron="*/5 * * * *"
)