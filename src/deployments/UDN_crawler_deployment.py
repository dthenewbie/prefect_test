from prefect_github import GitHubRepository
from flows.UDN_crawler import UDN_news_scraper_pipeline


UDN_news_scraper_pipeline.from_source(
    source=GitHubRepository.load("github-prefect-demo"),
    entrypoint="src/flow/UDN_crawler.py:UDN_news_scraper_pipeline",
).deploy(
    name="UDN_news_crawler_deployment",
    tags=["web crawler", "UDN", "case processing"],
    work_pool_name="test-docker",
    job_variables=dict(pull_policy="Never"),
    # parameters=dict(name="Marvin"),
    cron="*/5 * * * *"
)