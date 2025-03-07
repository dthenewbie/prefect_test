from prefect_github import GitHubRepository
from flows.Fraud_Line_ID_api_flow import Fraud_Line_ID_api

Fraud_Line_ID_api.from_source(
    source=GitHubRepository.load("antifrauddocker"),
    entrypoint="src/flows/Fraud_Line_ID_api_flow.py:Fraud_Line_ID_api",
).deploy(
        name="Fraud_Line_ID_api_docker",
        tags=["API", "Open Data", "Fraud_Line_ID"],
        work_pool_name="antifrauddocker",
        cron="0 18 * * *"
)