import requests
import time

def request_with_retry(url, retries=3, timeout=5) -> requests.Response:
    headers = {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36"}
    response = None
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            time.sleep(1) # 等待後重試
            if response.status_code != 200:
                print(f"請求失敗，status code: {response.status_code}")
                print(f"Retrying {url} (Attempt {attempt + 1}/{retries})")
                time.sleep(1)
                response = None
            else:
                break
        except requests.exceptions.Timeout:
            print(f"請求超時，Retrying {url} (Attempt {attempt + 1}/{retries})")
        except Exception as e:
            print(f"請求異常: {e}")
        time.sleep(1) # 等待後重試
    return response