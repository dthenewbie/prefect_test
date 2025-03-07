from prefect_github import GitHubRepository
from flows.PTT_crawler_flow import PTT_scraper_pipeline


PTT_scraper_pipeline.from_source(
    source=GitHubRepository.load("antifrauddocker"),
    entrypoint="src/flows/PTT_crawler_flow.py:PTT_scraper_pipeline",
).deploy(
        name="PTT_crawler_deployment_docker",
        tags=["web crawler", "PTT", "case processing"],
        work_pool_name="antifrauddocker",
        parameters=dict(pagenum = int(20)),
        cron="0 8 * * *"
)