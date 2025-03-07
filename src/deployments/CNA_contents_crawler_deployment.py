from prefect_github import GitHubRepository
from flows.CNA_contents_crawler_flow import CNA_news_scraper_pipeline


CNA_news_scraper_pipeline.from_source(
    source=GitHubRepository.load("antifrauddocker"),
    entrypoint="src/flows/CNA_contents_crawler_flow.py:CNA_news_scraper_pipeline",
).deploy(
        name="CNA_Contents_Crawler_deployment_docker",
        tags=["web crawler", "CNA", "case processing"],
        work_pool_name="antifrauddocker",
        cron="0 12 * * *"
)