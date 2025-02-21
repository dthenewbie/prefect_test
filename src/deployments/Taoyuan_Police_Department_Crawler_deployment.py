from prefect_github import GitHubRepository
from flows.Taoyuan_Police_Department_Crawler_flow import Taoyuan_Police_Department_scraper_pipeline


Taoyuan_Police_Department_scraper_pipeline.from_source(
    source=GitHubRepository.load("antifraud"),
    entrypoint="src/flows/Taoyuan_Police_Department_Crawler_flow.py:Taoyuan_Police_Department_scraper_pipeline",
).deploy(
    name="Taoyuan_Police_Department_crawler_deployment",
    tags=["web crawler", "Taoyuan_Police_Department_Crawler", "case processing"],
    work_pool_name="antifraud",
    job_variables=dict(pull_policy="Never"),
    # parameters=dict(name="Marvin"),
    cron="0 10 * * 3",
    timezone="Asia/Taipei"
)