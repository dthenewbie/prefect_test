from prefect_github import GitHubRepository
from flows.ntpd_crawler import New_Taipei_Police_Department_scraper_pipeline


New_Taipei_Police_Department_scraper_pipeline.from_source(
    source=GitHubRepository.load("github-prefect-demo"),
    entrypoint="src/flow/ntpd_crawler.py:New_Taipei_Police_Department_scraper_pipeline",
).deploy(
    name="New_Taipei_Police_Department_crawler_deployment",
    tags=["web crawler", "New_Taipei_Police_Department", "case processing"],
    work_pool_name="test-docker",
    job_variables=dict(pull_policy="Never"),
    # parameters=dict(name="Marvin"),
    cron="*/5 * * * *",
)