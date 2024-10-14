import asyncio
import aiohttp
import re
import asyncpg
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def request(session, url):
    try:
        async with session.get(url) as response:
            response.raise_for_status()  
            return await response.json()
    except aiohttp.ClientResponseError as e:
        logger.error(f"Failed to fetch data from: {url}. Error: {e}")
        return None

async def insert_person(conn, person_data):
    try:
        await conn.execute(
            """
            INSERT INTO people (
                id, birth_year, eye_color, films, gender, hair_color, height, homeworld, mass, name, skin_color, species, starships, vehicles
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
            )
            """,
            int(person_data['id']), person_data['birth_year'], person_data['eye_color'], person_data['films'],
            person_data['gender'], person_data['hair_color'], person_data['height'], person_data['homeworld'],
            person_data['mass'], person_data['name'], person_data['skin_color'], person_data['species'],
            person_data['starships'], person_data['vehicles']
        )
        logger.info(f"Inserted person: {person_data['name']}")
    except Exception as e:
        logger.error(f"Failed to insert person: {person_data['name']}. Error: {e}")

async def fetch_and_insert_person(session, conn, person):
    person_url = person['url']
    person_id_match = re.search(r'people/(\d+)/', person_url)
    if person_id_match:
        person_id = person_id_match.group(1)
        
        try:
            films = ', '.join([film['title'] for film in await asyncio.gather(*[request(session, film_url) for film_url in person['films']]) if film])
            species = ', '.join([specie['name'] for specie in await asyncio.gather(*[request(session, specie_url) for specie_url in person['species']]) if specie])
            starships = ', '.join([starship['name'] for starship in await asyncio.gather(*[request(session, starship_url) for starship_url in person['starships']]) if starship])
            vehicles = ', '.join([vehicle['name'] for vehicle in await asyncio.gather(*[request(session, vehicle_url) for vehicle_url in person['vehicles']]) if vehicle])
            homeworld = ', '.join([planet['name'] for planet in await asyncio.gather(*[request(session, planet_url) for planet_url in [person['homeworld']]]) if planet])
        except Exception as e:
            logger.error(f"Failed to fetch additional data for person: {person['name']}. Error: {e}")
            return
        
        person_data = {
            'id': person_id,
            'birth_year': person['birth_year'],
            'eye_color': person['eye_color'],
            'films': films,
            'gender': person['gender'],
            'hair_color': person['hair_color'],
            'height': person['height'],
            'homeworld': homeworld,
            'mass': person['mass'],
            'name': person['name'],
            'skin_color': person['skin_color'],
            'species': species,
            'starships': starships,
            'vehicles': vehicles
        }
        
        await insert_person(conn, person_data)

async def main():
    base_url = 'https://swapi.dev/api/'
    API_list = ['people', 'films', 'starships', 'vehicles', 'species', 'planets']
    
    async with aiohttp.ClientSession() as session:
        try:
            conn = await asyncpg.connect(
                user='postgres',
                password='7Ff3imRYmE',
                database='swapi_db',
                host='localhost',
                port=5432
            )
            logger.info("Connected to the database successfully.")
        except Exception as e:
            logger.error(f"Failed to connect to the database. Error: {e}")
            return
        
        for i in API_list:
            url = base_url + i
            while url:
                result = await request(session, url)
                if result is None:
                    break
                
                logger.info(f"Fetched data from: {url}")
                
                if i == 'people':
                    tasks = []
                    for person in result['results']:
                        tasks.append(asyncio.create_task(fetch_and_insert_person(session, conn, person)))
                    await asyncio.gather(*tasks)
                
                url = result.get('next')
    
    await conn.close()
    logger.info("Disconnected from the database.")

if __name__ == '__main__':
    asyncio.run(main())