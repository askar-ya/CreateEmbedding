from psql import Psql
import time
from logic import log, make_embedding
import asyncio

async def main():
    while True:
        psql = Psql()
        state = psql.update_embedding_task()
        psql.close()

        if state == 'running':

            psql = Psql()
            reels = psql.get_reel_for_embedding()
            psql.close()

            workers = []
            for reel in reels:
                reel_id, reel_uri, description, transcribe = reel

                log(f"Reel: {reel_id}")
                print(f"Reel: {reel_id}")

                text = description + ' ' + transcribe

                workers.append(make_embedding(text, reel_id))

            if workers:
                psql = Psql()
                results = await asyncio.gather(*workers)

                for result in results:
                    if result[0]:
                        psql.wright_embedding(result[0], result[1])
                psql.close()
                log(f"done")
                print(f"done")

        else:
            time.sleep(60 * 60 * 2)

asyncio.run(main())