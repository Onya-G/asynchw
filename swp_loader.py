import aiohttp
import asyncio
from more_itertools import chunked
import asyncpg
import config


async def get_swp(session: aiohttp.ClientSession, swp_id):
    async with session.get(f'https://swapi.dev/api/people/{swp_id}/') as response:
        return await response.json()


def to_tuple(swp_jsons):
    swp_tuples = []
    for i in swp_jsons:
        if i.get('detail'):
            continue
        else:
            swp_tuples.append((i['name'], i['gender'], i['birth_year'], i['eye_color'], ', '.join(i['films']),
                               i['hair_color'], i['height'], i['homeworld'], i['mass'], i['skin_color'],
                               ', '.join(i['species']), ', '.join(i['starships']), ', '.join(i['vehicles'])))
    return swp_tuples


async def insert_swp(pool: asyncpg.Pool, swp_tuples):
    query = 'INSERT INTO swpeople (name, gender, birth_year, eye_color, films, hair_color, height, homeworld, mass,' \
            ' skin_color, species, starships, vehicles) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)'
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(query, swp_tuples)


async def main():
    async with aiohttp.ClientSession() as session:
        for chunk in chunked(range(1, 20), 5):
            swp_get_tasks = [asyncio.create_task(get_swp(session, i)) for i in chunk]
            result = await asyncio.gather(*swp_get_tasks)

            pool = await asyncpg.create_pool(config.PG_DSN, min_size=20, max_size=20)
            swp_insert_tasks = []
            for swp_chunk in chunked(to_tuple(result), 5):
                swp_insert_tasks.append(asyncio.create_task(insert_swp(pool, swp_chunk)))

            await asyncio.gather(*swp_insert_tasks)
            await pool.close()


if __name__ == '__main__':
    asyncio.run(main())
