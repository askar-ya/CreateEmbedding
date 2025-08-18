from openai import OpenAI
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime
import httpx

load_dotenv()
token = os.getenv('OpenAiToken')

def log(*kwargs):
    out_file = 'logs.txt'
    out = ''
    for kwarg in kwargs:
        out += f' {kwarg}'

    if os.path.exists(out_file):
        mode = 'a'
    else:
        mode = 'w'

    with open(out_file, mode=mode, encoding='utf-8') as f:
        now = datetime.now()
        date_str = f"[ {now.day}.{now.month} | {now.hour}:{now.minute} ] "
        f.write(date_str + out + "\n")


def make_embedding(text: str):
    # Создает векторы из текста
    client = OpenAI(api_key=token, http_client=httpx.Client(proxy='socks5://HDR7yg:wh74ML@147.45.200.146:8000'))

    response = client.embeddings.create(
        input=text,
        model="text-embedding-3-large"
    )
    return response.data[0].embedding


def search_embedding(text: str):
    q_embedding = make_embedding(text)

    con = psycopg2.connect(
        host="localhost", database=os.getenv("PSQL_NAME"),
        user=os.getenv("PSQL_USER"), password=os.getenv("PSQL_PASS"))

    con.autocommit = True
    cur = con.cursor()
    cur.execute(f"SELECT id, embedding, embedding <-> '{q_embedding}' AS similarity "
                f"FROM reel_embeddings "
                f"ORDER BY embedding <-> '{q_embedding}' "
                f"LIMIT 3")

    output = cur.fetchall()
    print(output)
