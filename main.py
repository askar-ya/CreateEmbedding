from psql import Psql
import time
from logic import log, make_embedding


while True:
    psql = Psql()
    state = psql.update_embedding_task()
    if state == 'running':
        reel_id, reel_uri, description, transcribe = psql.get_reel_for_embedding()
        log(f"Reel: {reel_id}")
        print(f"Reel: {reel_id}")


        text = description + ' ' + transcribe

        embedding = make_embedding(text)
        psql.wright_embedding(embedding, reel_id)


        psql.close()

    else:
        psql.close()
        time.sleep(60 * 60 * 2)