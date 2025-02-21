from prefect_github import GitHubRepository
from flows.PTT_crawler_flow import PTT_scraper_pipeline


PTT_scraper_pipeline.from_source(
    source=GitHubRepository.load("antifraud"),
    entrypoint="src/flows/PTT_crawler_flow.py:PTT_scraper_pipeline",
).deploy(
    name="PTT_crawler_deployment",
    tags=["web crawler", "PTT", "case processing"],
    work_pool_name="antifraud",
    job_variables=dict(pull_policy="Never"),
    parameters=dict(pagenum = int(20)),
    cron="0 8 * * *",
    timezone="Asia/Taipei"
)