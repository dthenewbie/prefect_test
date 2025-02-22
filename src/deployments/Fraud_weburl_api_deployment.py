from prefect_github import GitHubRepository
from flows.Fraud_weburl_api_flow import Fraud_Weburl_api

Fraud_Weburl_api.from_source(
source=GitHubRepository.load("antifraud"),
entrypoint="src/flows/Fraud_weburl_api_flow.py:Fraud_Weburl_api",
).deploy(
    name="Fraud_Weburl_api",
    tags=["API", "Open Data", "Fraud_Weburl"],
    work_pool_name="antifraud",
    job_variables=dict(pull_policy="Never"),
    # parameters=dict(name="Marvin"),
    cron="0 18 * * *"
)