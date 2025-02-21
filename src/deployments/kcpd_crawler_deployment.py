from prefect_github import GitHubRepository
from flows.kcpd_crawler_flow import Kaohsiung_Police_Department_scraper_pipeline


Kaohsiung_Police_Department_scraper_pipeline.from_source(
    source=GitHubRepository.load("antifraud"),
    entrypoint="src/flows/kcpd_crawler_flow.py:Kaohsiung_Police_Department_scraper_pipeline",
).deploy(
    name="Kaohsiung_Police_Department_crawler_deployment",
    tags=["web crawler", "Kaohsiung_Police_Department", "case processing"],
    work_pool_name="antifraud",
    job_variables=dict(pull_policy="Never"),
    # parameters=dict(name="Marvin"),
    cron="0 9 * * 3"
)