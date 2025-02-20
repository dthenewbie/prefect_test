import pymysql
def connect_db():
    try:
        conn = pymysql.connect(
        host = '104.199.228.45',
        port = 3306,
        user = 'root',
        passwd = 'my-secret-pw',
        db = 'Anti_Fraud'
          )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None