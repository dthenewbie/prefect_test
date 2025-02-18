from prefect_github import GitHubRepository
from flows.PTT_crawler import PTT_scraper_pipeline


PTT_scraper_pipeline.from_source(
    source=GitHubRepository.load("github-prefect-demo"),
    entrypoint="src/flow/PTT_crawler.py:PTT_scraper_pipeline",
).deploy(
    name="PTT_crawler_deployment",
    tags=["web crawler", "PTT", "case processing"],
    work_pool_name="test-docker",
    job_variables=dict(pull_policy="Never"),
    # parameters=dict(name="Marvin"),
    cron="*/5 * * * *"
)