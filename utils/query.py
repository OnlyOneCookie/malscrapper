import os
import json
import psycopg2

from enum import Enum

ROOT_FOLDER = os.path.dirname(
    os.path.abspath(__file__)
)
EXPORT_FOLDER_QUERIES = f'{ROOT_FOLDER}/../queries'
EXPORT_FOLDER_DATA = f'{ROOT_FOLDER}/../data'
DB_LIST_ANIME = f'{EXPORT_FOLDER_DATA}/db_anime.json'
DB_LIST_MANGA = f'{EXPORT_FOLDER_DATA}/db_manga.json'

DB_CONFIG = {
    "host": "localhost",
    "database": "sololeveling",
    "user": "solo",
    "password": "leveling",
    "port": 5432
}

DB_ADMIN_CONFIG = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "postgres",
    "port": 5432
}

class ENTITY(Enum):
    GENRE = 'genre'
    MEDIA_TYPE = 'media_type'
    NSFW = 'nsfw'
    RATING = 'rating'
    STATUS = 'status'
    SOURCE = 'source'
    SEASON = 'season'
    STUDIO = 'studio'
    STATISTICS = 'statistics'
    ANIME = 'anime'
    MANGA = 'manga'

    def __repr__(self):
        return self.value

    def __str__(self):
        return self.value
    
    def __eq__(self, other):
        if isinstance(other, ENTITY):
            return self.value == other.value
        if isinstance(other, str):
            return self.value == other
        return False

    def __ne__(self, other):
        return not self.__eq__(other)
    
entities = ENTITY

def generator():
    prep()

    animes = get_data(DB_LIST_ANIME)
    mangas = get_data(DB_LIST_MANGA)
    print(f'Generating queries with {len(animes)} anime entries and {len(mangas)} manga entries.')

    for entity in entities:
        print(f'Generating query for {entity}...')

        if entity == ENTITY.GENRE:
            genres = []
            for i in animes + mangas:
                j = i.get('genres', [])
                for genre in j:
                    genres.append(genre)
            
            # Remove duplicates and sort list
            seen = set()
            entries = sorted([x for x in genres if not (tuple(x.items()) in seen or seen.add(tuple(x.items())))], key=lambda x: x['id'])
            
            sql = f'INSERT INTO {entity}(id, name) VALUES(%s, %s);'
            for entry in entries:
                with psycopg2.connect(**DB_CONFIG) as db:
                    with db.cursor() as cursor:
                        id = entry.get('id')
                        name = entry.get('name')
                        cursor.execute(sql, (id, name,))

                        db.commit()
        elif entity == ENTITY.MEDIA_TYPE:
            pass
        elif entity == ENTITY.NSFW:
            pass
        elif entity == ENTITY.RATING:
            pass
        elif entity == ENTITY.STATUS:
            pass
        elif entity == ENTITY.SOURCE:
            pass
        elif entity == ENTITY.SEASON:
            pass
        elif entity == ENTITY.STUDIO:
            pass
        elif entity == ENTITY.STATISTICS:
            pass
        elif entity == ENTITY.ANIME:
            pass
        elif entity == ENTITY.MANGA:
            pass
        else:
            print(f'Entity not supported: {entity}')

def prep():
    print(f'Preparing database...')

    sql = f'''
DROP DATABASE IF EXISTS sololeveling FORCE;
CREATE DATABASE sololeveling
    WITH
    OWNER = solo
    ENCODING = 'UTF8'
    LOCALE_PROVIDER = 'libc'
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;
'''
    with psycopg2.connect(**DB_ADMIN_CONFIG) as db:
        with db.cursor() as cursor:
            cursor.execute(sql)
            db.commit()

def get_data(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        return data
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        return None
    except json.JSONDecodeError:
        print(f"Error: The file '{file_path}' does not contain valid JSON.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        return None
