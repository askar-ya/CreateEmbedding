import psycopg2
from dotenv import load_dotenv
import os
from datetime import datetime
from datetime import timezone
utc_tz = timezone.utc

load_dotenv()

reels_page = '10'

class Psql:
    """Класс для работы с базой данных Postgresql"""

    def __init__(self):
        """Инициализируем подключение к базе данных"""
        self.connection = psycopg2.connect(user=os.getenv('PSQL_USER'),
                                           password=os.getenv('PSQL_PASS'),
                                           dbname=os.getenv('PSQL_NAME'),
                                           host=os.getenv('PSQL_HOST'),
                                           port=os.getenv('PORT_DB'))

        self.connection.autocommit = True  # Включаем автокомит
        self.cursor = self.connection.cursor()


    def close(self):
        self.cursor.close()
        self.connection.close()


    def get(self, table: str, columns: list = False, q: dict = None) -> list:
        """Метод получения данных колонки (некоторых или всех) из таблицы"""

        columns = columns if columns is False else ', '.join(columns)

        request = f"select {columns} from {table}"
        if q is not None:
            request += ' where '
            for n, value in enumerate(q):
                if type(q[value]) == str:
                    p_value = f"'{q[value]}'"
                else:
                    p_value = q[value]
                request += f"{value} =  {p_value}"
                if n < len(q) - 1:
                    if len(q) != 1:
                        request += ' AND '

        self.cursor.execute(request)
        return self.cursor.fetchall()

    def get_all_reels(self):
        reels_id = self.get('reels', columns=['id'])
        print(reels_id)

    def update_embedding_task(self):
        task_id, state = self.get('parser_history', columns=['id', 'state'], q={'task_type': 'make_embedding'})[0]
        all_reels = self.get('reels', columns=['count(id)'])[0][0]
        done_reels = self.get('reel_embeddings', columns=['count(id)'])[0][0]

        now = datetime.now(utc_tz)

        if state == 'running':
            if all_reels == done_reels:
                state = 'finished'
                q = f"update parser_history set (stop_time, state, reels_added, reels_total) = ('{now}', '{state}', {done_reels}, {all_reels}) where id = {task_id} "
            else:
                q = f'update parser_history set (reels_added, reels_total) = ({done_reels}, {all_reels}) where id = {task_id} '

        else:
            state = 'running'
            q = f"update parser_history set (start_time, state, reels_added, reels_total) = ('{now}', '{state}', {done_reels}, {all_reels}) where id = {task_id} "

        self.cursor.execute(q)
        return state

    def get_reel_for_embedding(self):
        q = 'SELECT id, url, description FROM reels r WHERE NOT EXISTS ( SELECT 1 FROM reel_embeddings WHERE reel_embeddings.reel_id = r.id ) order by id limit ' + reels_page
        self.cursor.execute(q)
        reels = self.cursor.fetchall()

        out = []

        for reel in reels:
            reel_id, url, description = reel[0], reel[1], reel[2]

            q = f'SELECT text FROM reel_transcriptions where reel_id = {reel_id} '
            self.cursor.execute(q)
            transcribe = self.cursor.fetchall()[0]

            if transcribe:
                transcribe = transcribe[0]
                if transcribe in ['', ' ', 'error_save']:
                    transcribe = ''

            out.append([reel_id, url, description, transcribe])

        return out

    def wright_embedding(self, embedding: list[float], reel_id: int):
        self.cursor.execute(f"INSERT INTO reel_embeddings (reel_id, embedding) VALUES ({reel_id}, '{embedding}')")

