from prefect import task
from utils.connect_db import connect_db
import pymysql
from prefect.blocks.notifications import SlackWebhook
@task
def save_to_caseprocessing(data: list, flow_name: str) -> None:
    """
    將資料存入 Case_processing 資料表
    format:
    [
        {
            "ID": str,
            "Title": str,
            "Reported_Date": str,
            "Content": str,
            "Url": str,
            "Area": str
        }
    ]
    """
    slack_webhook_block = SlackWebhook.load("flowcheck")
    insert_success_count = 0
    error_count = 0
    conn = connect_db()
    if conn is not None:
        with conn.cursor() as cursor:
            for record in data:
                try:
                    sql = """
                    INSERT INTO Case_processing
                    (ID, Title, Reported_Date, Content, Url, Area) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(sql, (
                        record['ID'], 
                        record['Title'], 
                        record['Reported_Date'], 
                        record['Content'], 
                        record['Url'], 
                        record['Area']
                    ))
                    insert_success_count += 1
                    # 錯誤計次歸0
                    error_count = 0
                except pymysql.IntegrityError as e:
                    if e.args[0] == 1062:
                        # print("Record already exists in the table.")
                        pass
                except Exception as e:
                    error_count += 1
                    print(f"| ERROR   | 【{flow_name}】 when save_to_caseprocessing: {e}")
                    if error_count == 1:
                        slack_webhook_block.notify(f"| ERROR   | 【{flow_name}】 when save_to_caseprocessing: {e}")
                    # 連續匯入失敗，判斷失敗
                    elif error_count > 25:
                        slack_webhook_block.notify(f"| CRITICAL| 【{flow_name}】 save_to_caseprocessing failed: {e}")
                        break
        conn.commit()
        slack_webhook_block.notify(f"| INFO    | 【{flow_name}】: Inserted successfully {insert_success_count}/{len(data)} into Case_processing.")
        print(f"| INFO    | 【{flow_name}】 : Inserted successfully {insert_success_count}/{len(data)} into Case_processing.")
    conn.close()