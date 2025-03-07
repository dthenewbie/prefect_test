from prefect_github import GitHubRepository
from flows.Fraud_trait_extractor_flow import trait_extractor_flow

trait_extractor_flow.from_source(
    source=GitHubRepository.load("antifrauddocker"),
    entrypoint="src/flows/Fraud_trait_extractor_flow.py:trait_extractor_flow",
).deploy(
        name="Fraud_case_trait_extractor",
        tags=["extractor", "Fraud_case", "Fraud_classification"],
        work_pool_name="antifrauddocker",
        parameters=dict(rounds= int(100)),
        cron="0 20 * * *"
)