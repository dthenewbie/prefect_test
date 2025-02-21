from prefect_github import GitHubRepository
from flows.ntpd_crawler_flow import New_Taipei_Police_Department_scraper_pipeline


New_Taipei_Police_Department_scraper_pipeline.from_source(
    source=GitHubRepository.load("antifraud"),
    entrypoint="src/flows/ntpd_crawler_flow.py:New_Taipei_Police_Department_scraper_pipeline",
).deploy(
    name="New_Taipei_Police_Department_crawler_deployment",
    tags=["web crawler", "New_Taipei_Police_Department", "case processing"],
    work_pool_name="antifraud",
    job_variables=dict(pull_policy="Never"),
    # parameters=dict(name="Marvin"),
    cron="0 11 * * 3",
    timezone="Asia/Taipei"
)