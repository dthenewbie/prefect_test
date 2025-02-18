import json
import pandas as pd
import pymysql
from prefect import flow, task
from prefect.blocks.notifications import SlackWebhook
import re
import logging
import time
import uuid
from langchain_community.chat_models import ChatOpenAI
from langchain.schema import SystemMessage, HumanMessage
from langchain_community.callbacks import get_openai_callback
from dotenv import load_dotenv

load_dotenv()

slack_webhook_block = SlackWebhook.load("flowcheck")

# ----- 日誌設定 ----- #
logging.basicConfig(
    filename='etl_process2025-02-15.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ----- OpenAI 提取模組 ----- #
def clean_none_values(data) -> dict:
    """ 遍歷 JSON 結構，將 'None' 或 '' 轉換為 None。 """
    if isinstance(data, dict):
        return {k: clean_none_values(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [clean_none_values(v) for v in data]
    elif data == "None" or data == "":
        return None
    return data

def clean_json_response(response_text) -> str:
    """ 移除 Markdown 標記和註解，確保 JSON 可解析 """
    response_text = re.sub(r"```json|```", "", response_text).strip()
    response_text = re.sub(r"#.*", "", response_text)  # 移除 JSON 內部的註解
    return response_text

def clean_content(text) -> str:
    # 去除特殊符號
    cleaned_text = re.sub(r'[^a-zA-Z0-9\u4e00-\u9fff\s.,。，!?！？;:：、()（）【】「」《》“”‘’]', '', text)
    # 去除多餘空白行
    cleaned_text = re.sub(r'\n+', '', cleaned_text).strip()
    return cleaned_text

class FraudContentExtractor:
    def __init__(self, model="gpt-4o-mini", response_format="json_object"):
        """
        初始化 FraudContentExtractor 類別。

        :param model: 使用的 OpenAI 模型名稱。
        :param response_format: 返回的格式（預設為 json_object）。
        """
        self.chat = ChatOpenAI(model=model, model_kwargs={"response_format": {"type": response_format}},timeout=20)
        self.system_message = SystemMessage(
            content="""
            You are tasked with extracting specific features from news articles related to fraud cases.
            Provide the extracted information strictly in JSON format without any comments or additional text.
            Do not include explanations, footnotes, or Markdown formatting. Return only valid JSON.

            1. **Area (受害者區域)**: *type: string or None*  
            Extract the victim's residential area mentioned in the article.  
            The area must be one of Taiwan's administrative divisions (直轄縣、市) from the following list:  
            臺北市、新北市、桃園市、臺中市、臺南市、高雄市、新竹縣、苗栗縣、彰化縣、南投縣、雲林縣、嘉義縣、屏東縣、宜蘭縣、花蓮縣、臺東縣、澎湖縣、金門縣、連江縣、基隆市、新竹市、嘉義市。  
            - If the text mentions a **district (區) or township (鄉、鎮、市)**, map it to the correct **直轄縣市**.
            - Example: **"板橋區"** → **"新北市"**
            - Example: **"中壢區"** → **"桃園市"**
            - If the **specific county or city cannot be determined**, return the **country name** (e.g., `"台灣"`, `"日本"`, `"韓國"`).  
            - If no location information is available, return **None** (not `"None"` or `""`).

            2. **Platform (詐騙管道)**: *type: string or None*  
            Identify the platform through which the victim contacted the fraudster.  
            Choose from the following options: 電話, 通訊軟體, 社群軟體, 交友軟體, 遊戲軟體, 連結.  
            If the information is not available, return **None(not "None" or "")**.

            3. **Victim_Gender (受害者性別)**: *type: string or None*  
            Identify the victim's gender. Use "M" for male and "F" for female.  
            If the information is not available, return **None(not "None" or "")**.

            4. **Victim_Age (受害者年齡)**: *type: int or None*  
            Extract the victim's age from the article.  
            If the information is not available, return **None(not "None" or "")**.

            5. **Victim_Career (受害者職業)**: *type: string or None*  
            Classify the victim's occupation into one of the following categories:  
            - 學生  
            - 工程師  
            - 業務/銷售  
            - 服務業  
            - 金融從業人員  
            - 公務員  
            - 自營商/創業者  
            - 家庭主婦/主夫  
            - 退休人士  
            - 無業/待業中  
            - 技術人員  
            - 醫療人員  
            - 藝術/娛樂業  
            - 其他  
            If the information is not available, return **None(not "None" or "")**.

            6. **Financial_Loss (被詐騙金額)**: *type: int or None*  
            Extract the total financial loss suffered by the victim.  
            Return the value as an integer without commas.  
            If the information is not available, return **None(not "None" or "")**.

            7. **Fraud_type (詐騙類型)**: *type: list or None*  
            Identify the type(s) of fraud involved in the case.  
            Return the corresponding **ID number(s)** based on the following mapping:  
            - 1: 假投資  
            - 2: 假交友  
            - 3: 網購  
            - 4: 遊戲  
            - 5: 釣魚簡訊  
            - 6: 假求職  
            - 7: 假檢警  
            - 8: 假中獎  
            - 9: 假冒身分  
            - 10: 宗教詐騙  
            - 11: 色情應召  
            - 12: 假貸款  
            - 13: 假推銷  
            - 14: 騙取金融帳戶
            - 15: 繳費詐騙
            It is possible for an article to involve multiple fraud types, like [1, 2].
            **Please return list with at least one most relevant Fraud_type_ID like [1].**
            Do not return **None "None" or ""** or **empty list []**. 

            8. **Is_Fraud (是否為詐騙文章)**: *type: int (1 or 0)*  
            Determine whether the article is related to fraud.  
            Return 1 if it is fraud-related, or 0 if it is not.
            You must identify whether the article is related to fraud or not. Do not return **None "None" or ""**.
            ---

            ### ⚠️ **Important Guidelines:**  
            - All extracted content must be in **Traditional Chinese (繁體中文)**, except for **Fraud_type**, which should return numeric IDs.  
            - The output must be in **strict JSON format** without any additional text.  
            - **Do NOT** add, remove, or modify the specified fields.  
            - If any feature is unavailable, **return None (not "None" or "")** in JSON.

            ---

            ### ✅ **Sample JSON Output**
            {
            "Area": "臺北市",
            "Platform": "社群軟體",
            "Victim_Gender": "F",
            "Victim_Age": 32,
            "Victim_Career": "學生",
            "Financial_Loss": 50000,
            "Fraud_type": [1, 2],
            "Is_Fraud": 1
            }
            ### **Sample JSON Output when data is unavailable**
            {
            "Area": None,
            "Platform": None,
            "Victim_Gender": None,
            "Victim_Age": None,
            "Victim_Career": None,
            "Financial_Loss": None,
            "Fraud_type": [],
            "Is_Fraud": 0
            }
            """
        )

    def process_content(self, case_id: str, contents: str, retries: int = 3)  -> dict:
        """
        將文章內容轉換為結構化格式。

        :param contents: 原始文章內容。
        :return: 結構化 JSON 格式數據。
        """
        user_message = HumanMessage(content=contents)
        #重試機制
        for attempt in range(retries):
            try:
                with get_openai_callback() as cb:
                    # 呼叫模型進行處理
                    response = self.chat.invoke([
                        self.system_message,
                        user_message
                    ])
                    cleaned_response = clean_json_response(response.content)  # 移除 Markdown 標記
                    # 嘗試解析為 JSON 格式
                    json_object = json.loads(cleaned_response)
                    # 清理 "None" 或 ""，確保回傳 Python None
                    json_object = clean_none_values(json_object)
                    logging.info(f"Total tokens used in this batch: {cb}")
                return json_object
            # except json.decoder.JSONDecodeError:
            except Exception as e:
                print(f"Error parsing JSON response: {e}")
                logging.error(f"Error parsing JSON response: {e}")
                if attempt < retries - 1:
                    time.sleep(1)  # 短暫等待後重試
                    print(f"{case_id} Retrying extraction {attempt + 1}/{retries}...")
                    logging.info(f"{case_id} Retrying extraction {attempt + 1}/{retries}...")
                    continue
            # 如果解析失敗，返回原始內容
            return {"error": "Failed to parse JSON response.", "raw_response": response.content}


# ----- MySQL 資料處理模組 ----- #
class MySQLHandler:
    def __init__(self, host, port, user, password, database):
        try:
            self.conn = pymysql.connect(
                host=host, port=port, user=user, password=password, database=database
            )
            self.cursor = self.conn.cursor()
            print("Database connection established successfully.")
            logging.info("Database connection established successfully.")
        except pymysql.MySQLError as e:
            slack_webhook_block.notify(f"| CRITICAL| flow 【trait_extractor】 Database connection failed: {e}")
            print(f"Database connection failed: {e}")
            logging.critical(f"Database connection failed: {e}")
            raise

    def fetch_unprocessed_cases(self) -> tuple:
        try:
            self.cursor.execute("SELECT ID, Title, Reported_Date, Content, Url, Area FROM Case_processing WHERE Status = 0")
            return self.cursor.fetchall()
        except pymysql.MySQLError as e:
            print(f"Error fetching data: {e}")
            logging.error(f"Error fetching data: {e}")
            return []
        


    def batch_insert_fraud_cases(self, fraud_cases) -> list:
        """
        批量寫入 Fraud_case 資料表。
        """
        insert_query = """
            INSERT INTO Fraud_case (Case_ID, Title, Reported_Date, Area, Platform, Victim_Gender, 
                                    Victim_Age, Victim_Career, Financial_Loss, Content, Url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        success_cases = []
        for case in fraud_cases:
            try:
                self.cursor.execute(insert_query, case)
                success_cases.append(case[0])  # 記錄成功插入的 Case_ID
            except pymysql.MySQLError as e:
                print(f"Error inserting case {case[0]}: {e}")
                logging.error(f"Error inserting case {case[0]}: {e}")
        return success_cases

    def batch_insert_fraud_classifications(self, fraud_classifications, valid_case_ids) -> set:
        """
        批量寫入 Fraud_classification 的各文章 Fraud_type。
        """
        insert_query = "INSERT INTO Fraud_classification (Case_ID, Fraud_type_ID) VALUES (%s, %s)"
        success_cases = set()
        for case_id, fraud_type_id in fraud_classifications:
            if case_id in valid_case_ids:
                try:
                    self.cursor.execute(insert_query, (case_id, fraud_type_id))
                    success_cases.add(case_id)
                except pymysql.MySQLError as e:
                    print(f"Error inserting fraud classifications for case {case_id}: {e}")
                    logging.error(f"Error inserting fraud classifications for case {case_id}: {e}")
        return success_cases
    
    def batch_update_non_fraud_cases(self, updates):
        """
        更新非詐騙文章的 Case_processing 表中的 Status 欄位和 Is_Fraud 欄位。
        """
        update_query = "UPDATE Case_processing SET Status = 1 WHERE ID = %s AND Is_Fraud = 0"
        for update in updates:
            try:
                self.cursor.execute(update_query, (update,))
            except pymysql.MySQLError as e:
                print(f"Error updating non-fraud case {update}: {e}")
                logging.error(f"Error updating non-fraud case {update}: {e}")

    def batch_update_case_processing(self, updates, valid_case_ids: set) -> list:
        """
        更新詐騙文章的 Case_processing 表中的 Status 和 Is_Fraud 欄位。
        """
        success_inputs = []
        update_query = "UPDATE Case_processing SET Status = 1, Is_Fraud = %s WHERE ID = %s"
        for update in updates:
            if update[1] in valid_case_ids:
                try:
                    self.cursor.execute(update_query, update)
                    success_inputs.append(update[1])
                except pymysql.MySQLError as e:
                    print(f"Error updating case processing: {e}")
                    logging.error(f"Error updating case processing: {e}")
        return len(success_inputs)

    def commit(self):
        """
        commit 資料異動
        """
        try:
            self.conn.commit()
            print("Database changes committed successfully.")
            logging.info("Database changes committed successfully.")
        except pymysql.MySQLError as e:
            print(f"Error during commit: {e}")
            logging.error(f"Error during commit: {e}")

    def close(self):
        """
        關閉資料庫連線
        """
        self.cursor.close()
        self.conn.close()
        print("Database connection closed.")
        logging.info("Database connection closed.")


# # ----- ETL 主流程 ----- #
# def main():
#     for _ in range(300):
#         try:
            
            
@task
def Extract_from_Fraud_case():
    db = MySQLHandler(host='host.docker.internal',
                    port=3306, 
                    user='root', 
                    password='password', 
                    database='Anti_Fraud')
    cases = db.fetch_unprocessed_cases()[:20]
    print(f"Fetched {len(cases)} unprocessed cases.")
    logging.info(f"Fetched {len(cases)} unprocessed cases.")
    db.close()
    return cases
@task
def openai_trait_extractor(cases: tuple):
    if len(cases) == 0 :
        print("No unprocessed cases found.")
        logging.info("No unprocessed cases found.")
        return [], [], [], []
    extractor = FraudContentExtractor()
    transformed_data = []
    fraud_classifications = []
    case_updates = []
    non_fraud_cases = []

    for case in cases:
        case_id, title, reported_date, content, url, area = case
        try:
            extracted_data = extractor.process_content(case_id, content)
            print(extracted_data)
            if 'error' not in extracted_data:
                if extracted_data.get('Is_Fraud', 0) == 0:
                    non_fraud_cases.append(case_id)
                else:
                    fraud_case_data = (
                        case_id or str(uuid.uuid4()), 
                        # title,
                        clean_content(title), 
                        reported_date, 
                        area if area else extracted_data.get('Area', None),
                        extracted_data.get('Platform', None), 
                        extracted_data.get('Victim_Gender', None),
                        extracted_data.get('Victim_Age', None), 
                        extracted_data.get('Victim_Career', None),
                        extracted_data.get('Financial_Loss', None), 
                        # content, 
                        clean_content(content),
                        url
                    )
                    transformed_data.append(fraud_case_data)
                    # if extracted_data.get('Fraud_type') is not List
                    if isinstance(extracted_data.get('Fraud_type', []), list):
                        for fraud_type_id in extracted_data.get('Fraud_type', []):
                            fraud_classifications.append((case_id, fraud_type_id))
                    else:
                        pass
                    # Status = 1, Is_Fraud = 1
                    case_updates.append((1, case_id))
        except Exception as e:
            print(f"skipping case {case_id} due to error: {e}") 
            logging.error(f"skipping case {case_id} due to error: {e}")
    return transformed_data, fraud_classifications, case_updates, non_fraud_cases
@task
def load_to_Anti_Fraud(transformed_data, fraud_classifications, case_updates, non_fraud_cases):
    if [transformed_data, fraud_classifications, case_updates, non_fraud_cases] == [[], [], [], []]:
        return slack_webhook_block.notify(f"| INFO    | flow 【trait_extractor】 No data to process.")
    try:
        db = MySQLHandler(host='host.docker.internal',
                        port=3306, 
                        user='root', 
                        password='password', 
                        database='Anti_Fraud')
        db.batch_update_non_fraud_cases(non_fraud_cases)
        valid_case_ids = db.batch_insert_fraud_cases(transformed_data)
        valid_classification_ids = db.batch_insert_fraud_classifications(fraud_classifications, valid_case_ids)
        success_inputs = db.batch_update_case_processing(case_updates, valid_classification_ids)
        db.commit()
        print(f"fraud:{success_inputs}/non_fraud:{len(non_fraud_cases)} data processed and committed successfully.")
        logging.info(f"fraud:{success_inputs}/non_fraud:{len(non_fraud_cases)} data processed and committed successfully.")

    except Exception as e:
        logging.critical(f"ETL process failed: {e}")
                
    finally:
        db.close()
@flow(name = "trait_extractor")
def trait_extractor_flow():
    try:
        cases = Extract_from_Fraud_case()
        transformed_data, fraud_classifications, case_updates, non_fraud_cases = openai_trait_extractor(cases)
        load_to_Anti_Fraud(transformed_data, fraud_classifications, case_updates, non_fraud_cases)
    except Exception as e:
        slack_webhook_block.notify(f"| ERROR   | flow 【trait_extractor】 failed: {e}")

if __name__ == "__main__":
    trait_extractor_flow()

    # temporary local server of worker
    # trait_extractor_flow.serve(
    #     name="Fraud_case_trait_extractor",  # Deployment name. It create a temporary deployment.
    #     tags=["extractor", "Fraud_case", "Fraud_classification"],  # Filtering when searching on UI.
    #     # parameters={
    #     #     "goodbye": True
    #     # },  # Overwrite default parameters defined on hello_world_flow. Only for this deployment.
    #     # interval=60,  # Like crontab, "* * * * *"
    #     cron="* 18 * * *",
    # )
