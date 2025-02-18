from prefect_github import GitHubRepository
from flows.dashboard_crawler import dashboard_scraper_pipeline


dashboard_scraper_pipeline.from_source(
    source=GitHubRepository.load("github-prefect-demo"),
    entrypoint="src/flow/dashboard_crawler.py:dashboard_scraper_pipeline",
).deploy(
    name="165dashboard_crawler_deployment",
    tags=["web crawler", "165 dashboard", "case processing"],
    work_pool_name="test-docker",
    job_variables=dict(pull_policy="Never"),
    # parameters=dict(name="Marvin"),
    cron="*/5 * * * *"
)