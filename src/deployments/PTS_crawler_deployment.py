from prefect_github import GitHubRepository
from flows.PTS_crawler_flow import PTS_news_scraper_pipeline


PTS_news_scraper_pipeline.from_source(
    source=GitHubRepository.load("antifrauddocker"),
    entrypoint="src/flows/PTS_crawler_flow.py:PTS_news_scraper_pipeline",
).deploy(
        name="pts_news_crawler_deployment_docker",
        tags=["web crawler", "PTS", "case processing"],
        work_pool_name="antifrauddocker",
        parameters=dict(pagenum = int(20)),
        cron="0 13 * * *"
)