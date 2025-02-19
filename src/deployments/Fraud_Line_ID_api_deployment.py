from prefect_github import GitHubRepository
from flows.Fraud_Line_ID_api import Fraud_Line_ID_api

Fraud_Line_ID_api.from_source(
    source=GitHubRepository.load("antifraud"),
    entrypoint="src/flows/Fraud_Line_ID_api.py:Fraud_Line_ID_api",
).deploy(
    name="Fraud_Line_ID_api",
    tags=["API", "Open Data", "Fraud_Line_ID"],
    work_pool_name="antifraud",
    job_variables=dict(pull_policy="Never"),
    # parameters=dict(name="Marvin"),
    cron="0 18 * * *"
)