from prefect import flow, task
from prefect.blocks.notifications import SlackWebhook
import requests
from utils.connect_db import connect_db

slack_webhook_block = SlackWebhook.load("flowcheck")

def fetch_api_data(url) -> list[dict]:
    headers = {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36"}
    try:
        response = requests.get(headers=headers, url=url)
        response.raise_for_status()  # 檢查 HTTP 回應是否有錯誤
        data = response.json()
        
        if data.get("success"):
            records = data.get("result", {}).get("records", [])
            return records
        else:
            print("API 回應失敗")
            return []
    except requests.exceptions.RequestException as e:
        print(f"API 請求錯誤: {e}")
        return []
@task
def save_to_Fraud_Line_ID():
    api_url = "https://od.moi.gov.tw/api/v1/rest/datastore/A01010000C-001277-053"
    records = fetch_api_data(api_url)
    success_count = 0
    sql = """
    INSERT INTO Fraud_Line_ID (LINE_ID, Reported_Date)
    VALUES (%s, %s)
    """
    conn = connect_db()
    with conn.cursor() as cursor:
        for record in records:
            try:
                cursor.execute(sql, (record['帳號'], record['通報日期']))
                success_count += 1
            except Exception as e:
                continue
        conn.commit()
    conn.close()
    slack_webhook_block.notify(f"| INFO    | {success_count} new ID saved to Fraud_Line_ID successfully")

@flow(name="Fraud_Line_ID_api")
def Fraud_Line_ID_api():
    try:
        save_to_Fraud_Line_ID()
    except Exception as e:
        slack_webhook_block.notify(f"| ERROR   | flow 【Fraud_Line_ID_api】 failed: {e}")

if __name__ == "__main__":

    # Fraud_Line_ID_api()
    
    # temporary local server of worker
    Fraud_Line_ID_api.serve(
        name="Fraud_Line_ID_api",  # Deployment name. It create a temporary deployment.
        tags=["API", "Open Data", "Fraud_Line_ID"],  # Filtering when searching on UI.
        # parameters={
        #     "goodbye": True
        # },  # Overwrite default parameters defined on hello_world_flow. Only for this deployment.
        # interval=60,  # Like crontab, "* * * * *"
        cron="0 17 * * *",
    )