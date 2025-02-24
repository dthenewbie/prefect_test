from prefect_github import GitHubRepository
from flows.dashboard_crawler_flow import dashboard_scraper_pipeline


dashboard_scraper_pipeline.from_source(
    source=GitHubRepository.load("antifraud"),
    entrypoint="src/flows/dashboard_crawler_flow.py:dashboard_scraper_pipeline",
).deploy(
    name="165dashboard_crawler_deployment",
    tags=["web crawler", "165 dashboard", "case processing"],
    work_pool_name="antifraud",
    job_variables=dict(pull_policy="Never"),
    parameters=dict(scroll_round=int(20)),
    cron="0 10 * * *"
)