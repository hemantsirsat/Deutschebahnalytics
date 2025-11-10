import os
import psycopg
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

conn_string = os.getenv('DATABASE_URL')

def save_to_db(conn, date):
    cur = conn.cursor()
    cur.execute("INSERT INTO data_dates (date) VALUES (%s)", (date,))
    conn.commit()
    cur.close()

def main():
    conn = psycopg.connect(conn_string)
    # Fetch current date
    dt = datetime.now().date()

    save_to_db(conn, dt)
    conn.close()

if __name__ == "__main__":
    main()