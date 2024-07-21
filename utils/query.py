import os
import json
import psycopg2
import time
import dateutil.parser
from datetime import datetime

from enum import Enum

ROOT_FOLDER = os.path.dirname(
    os.path.abspath(__file__)
)
EXPORT_FOLDER_QUERIES = f'{ROOT_FOLDER}/../queries'
EXPORT_FOLDER_DATA = f'{ROOT_FOLDER}/../data'
DB_LIST_ANIME = f'{EXPORT_FOLDER_DATA}/db_anime.json'
DB_LIST_MANGA = f'{EXPORT_FOLDER_DATA}/db_manga.json'
TIME_START = None
TIME_CHECKPOINT = None
TIME_END = None

DB_CONFIG = {
    "host": "localhost",
    "database": "sololeveling",
    "user": "solo",
    "password": "leveling",
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
    AUTHOR = 'author'
    ANIME = 'anime'
    MANGA = 'manga'
    STATISTICS = 'statistics'

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
    TIME_START = time.time()
    TIME_CHECKPOINT = TIME_START
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
                    genres.append(genre['name'])
            
            genres = list(set(genres))
            
            entries = []
            for i in genres:
                entries.append({'id': len(entries) + 1, 'name': i})

            query = f'INSERT INTO {entity}(id, name) VALUES(%s, %s);'
            for entry in entries:
                with psycopg2.connect(**DB_CONFIG) as db:
                    db.autocommit = True
                    with db.cursor() as cursor:
                        id = entry.get('id')
                        name = entry.get('name')
                        cursor.execute(query, (id, name,))
        elif entity == ENTITY.MEDIA_TYPE:
            media_types = []
            for i in animes + mangas:
                j = i.get('media_type')
                if j:
                    media_types.append(j)

            media_types = list(set(media_types))
            media_types_str = ', '.join(f'\'{media_type}\'' for media_type in media_types)

            query = f'''
CREATE TYPE {entity} AS ENUM(
    {media_types_str}
);'''

            with psycopg2.connect(**DB_CONFIG) as db:
                db.autocommit = True
                with db.cursor() as cursor:
                    cursor.execute(query)

        elif entity == ENTITY.NSFW:
            pass
        elif entity == ENTITY.RATING:
            ratings = []
            for i in animes + mangas:
                j = i.get('rating')
                if j:
                    ratings.append(j)

            ratings = list(set(ratings))
            ratings_str = ', '.join(f'\'{rating}\'' for rating in ratings)
            query = f'''
CREATE TYPE {entity} AS ENUM(
    {ratings_str}
);'''
            
            with psycopg2.connect(**DB_CONFIG) as db:
                db.autocommit = True
                with db.cursor() as cursor:
                    cursor.execute(query)
        elif entity == ENTITY.STATUS:
            statuses = []
            for i in animes + mangas:
                j = i.get('status')
                if j:
                    statuses.append(j)

            statuses = list(set(statuses))
            statuses_str = ', '.join(f'\'{status}\'' for status in statuses)

            query = f'''
CREATE TYPE {entity} AS ENUM(
    {statuses_str}
);'''
            
            with psycopg2.connect(**DB_CONFIG) as db:
                db.autocommit = True
                with db.cursor() as cursor:
                    cursor.execute(query)
        elif entity == ENTITY.SOURCE:
            sources = []
            for i in animes + mangas:
                j = i.get('source')
                if j:
                    sources.append(j)
            
            sources = list(set(sources))
            sources_str = ', '.join(f'\'{source}\'' for source in sources)


            query = f'''
CREATE TYPE {entity} AS ENUM(
    {sources_str}
);'''
            
            with psycopg2.connect(**DB_CONFIG) as db:
                db.autocommit = True
                with db.cursor() as cursor:
                    cursor.execute(query)
        elif entity == ENTITY.SEASON:
            pass
        elif entity == ENTITY.STUDIO:
            studios = []
            for i in animes:
                j = i.get('studios', [])
                for studio in j:
                    studios.append(studio)

            seen = set()
            entries = sorted([x for x in studios if not (tuple(x.items()) in seen or seen.add(tuple(x.items())))], key=lambda x: x['id'])
            query = f'INSERT INTO {entity}(id, name) VALUES(%s, %s);'
            with psycopg2.connect(**DB_CONFIG) as db:
                db.autocommit = True
                with db.cursor() as cursor:
                    for entry in entries:
                        id = entry.get('id')
                        name = entry.get('name')
                        cursor.execute(query, (id, name,))
        elif entity == ENTITY.AUTHOR:
            pass
        elif entity == ENTITY.ANIME:
            with psycopg2.connect(**DB_CONFIG) as db:
                db.autocommit = True
                with db.cursor() as cursor:
                    query = f'''
CREATE TABLE IF NOT EXISTS {entity}(
    id int primary key not null, 
    title json,
    cover text,
    alternative_titles text[],
    start_date date,
    end_date date,
    synopsis text,
    nsfw nsfw,
    created_at timestamptz,
    updated_at timestamptz,
    media_type media_type,
    rating rating,
    related_anime json,
    related_manga json,
    status status,
    episodes int,
    season season
);'''
                    cursor.execute(query) # anime table

                    query = f'''
CREATE TABLE IF NOT EXISTS {entity}_{ENTITY.GENRE} (
    {entity}_id int not null,
    {ENTITY.GENRE}_id int not null,
    primary key ({entity}_id, {ENTITY.GENRE}_id),
    foreign key ({entity}_id) references {entity}(id) on delete cascade,
    foreign key ({ENTITY.GENRE}_id) references {ENTITY.GENRE}(id) on delete cascade
);'''
                    cursor.execute(query) # anime_genre table

                    query = f'''
CREATE TABLE IF NOT EXISTS {entity}_{ENTITY.STUDIO} (
    {entity}_id int not null,
    {ENTITY.STUDIO}_id int not null,
    primary key ({entity}_id, {ENTITY.STUDIO}_id),
    foreign key ({entity}_id) references {entity}(id) on delete cascade,
    foreign key ({ENTITY.STUDIO}_id) references {ENTITY.STUDIO}(id) on delete cascade
);'''
                    cursor.execute(query) # anime_studio table

            with psycopg2.connect(**DB_CONFIG) as db:
                db.autocommit = True
                with db.cursor() as cursor:
                    for i in animes:
                        id = i['id']
                        title_original = f"{i.get('title')}"
                        title_en = f"{i.get('alternative_titles', '').get('en', None)}"
                        title_jp = f"{i.get('alternative_titles', '').get('ja', None)}"
                        title = [{'original': title_original}, {'en': title_en}, {'jp': title_jp}]
                        title = json.dumps(title)
                        cover = i.get('main_picture', {}).get('large', None)
                        alternative_titles = [synonym for synonym in i.get('alternative_titles').get('synonyms', None)]
                        start_date = i.get('start_date')
                        if start_date is not None:
                            try:
                                bool(datetime.strptime(start_date, "%Y-%m-%d"))
                            except ValueError:
                                start_date = dateutil.parser.parse(start_date, dayfirst=True)
                        end_date = i.get('end_date')
                        if end_date is not None:
                            try:
                                bool(datetime.strptime(end_date, "%Y-%m-%d"))
                            except ValueError:
                                end_date = dateutil.parser.parse(end_date, dayfirst=True)
                        
                        synopsis = i.get('synopsis')
                        nsfw = i.get('nsfw')
                        created_at = i.get('created_at')
                        updated_at = i.get('updated_at')
                        media_type = i.get('media_type')
                        rating = i.get('rating')
                        related_anime = json.dumps(None)
                        related_manga = json.dumps(None)
                        status = i.get('status')
                        episodes = i.get('num_episodes')
                        season = i.get('start_season', {}).get('season', None)
                        query = f'INSERT INTO {entity} (id, title, cover, alternative_titles, start_date, end_date, synopsis, nsfw, rating, related_anime, related_manga, created_at, updated_at, media_type, status, episodes, season) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
                        cursor.execute(query, (id, title, cover, alternative_titles, start_date, end_date, synopsis, nsfw, rating, related_anime, related_manga, created_at, updated_at, media_type, status, episodes, season,))

                        # add genres to junction table
                        genres = [genre['name'] for genre in i.get('genres', [])]
                        for genre in genres:
                            query = f'SELECT id FROM {ENTITY.GENRE} WHERE name = %s'
                            cursor.execute(query, (genre,))
                            genre_id = cursor.fetchone()

                            if genre_id is not None:
                                query = f'INSERT INTO {entity}_{ENTITY.GENRE} ({entity}_id, {ENTITY.GENRE}_id) VALUES(%s, %s);'
                                cursor.execute(query, (id, genre_id,))

                        # add studios to junction table
                        studios = [studio['id'] for studio in i.get('studios', [])]
                        for studio in studios:
                            query = f'INSERT INTO {entity}_{ENTITY.STUDIO} ({entity}_id, {ENTITY.STUDIO}_id) VALUES(%s, %s);'
                            cursor.execute(query, (id, studio,))
        elif entity == ENTITY.MANGA:
            with psycopg2.connect(**DB_CONFIG) as db:
                db.autocommit = True
                with db.cursor() as cursor:
                    query = f'''
CREATE TABLE IF NOT EXISTS {entity}(
    id int primary key not null, 
    title json,
    cover text,
    alternative_titles text[],
    start_date date,
    end_date date,
    synopsis text,
    nsfw nsfw,
    created_at timestamptz,
    updated_at timestamptz,
    media_type media_type,
    status status,
    authors json,
    related_anime json,
    related_manga json,
    volumes int,
    chapters int,
    season season
);'''
                    cursor.execute(query) # manga table

                    query = f'''
CREATE TABLE IF NOT EXISTS {entity}_{ENTITY.GENRE} (
    {entity}_id int not null,
    {ENTITY.GENRE}_id int not null,
    primary key ({entity}_id, {ENTITY.GENRE}_id),
    foreign key ({entity}_id) references {entity}(id) on delete cascade,
    foreign key ({ENTITY.GENRE}_id) references {ENTITY.GENRE}(id) on delete cascade
);'''
                    cursor.execute(query) # manga_genre table

            with psycopg2.connect(**DB_CONFIG) as db:
                db.autocommit = True
                with db.cursor() as cursor:
                    for i in mangas:
                        id = i['id']
                        title_original = f"{i.get('title')}"
                        title_en = f"{i.get('alternative_titles', '').get('en', None)}"
                        title_jp = f"{i.get('alternative_titles', '').get('ja', None)}"
                        title = [{'original': title_original}, {'en': title_en}, {'jp': title_jp}]
                        title = json.dumps(title)
                        cover = i.get('main_picture', {}).get('large', None)
                        alternative_titles = [synonym for synonym in i.get('alternative_titles').get('synonyms', None)]
                        start_date = i.get('start_date')
                        if start_date is not None:
                            try:
                                bool(datetime.strptime(start_date, "%Y-%m-%d"))
                            except ValueError:
                                start_date = dateutil.parser.parse(start_date, dayfirst=True)
                        end_date = i.get('end_date')
                        if end_date is not None:
                            try:
                                bool(datetime.strptime(end_date, "%Y-%m-%d"))
                            except ValueError:
                                end_date = dateutil.parser.parse(end_date, dayfirst=True)
                        synopsis = i.get('synopsis')
                        nsfw = i.get('nsfw')
                        created_at = i.get('created_at')
                        updated_at = i.get('updated_at')
                        media_type = i.get('media_type')
                        authors = json.dumps(None)
                        status = i.get('status')
                        related_anime = json.dumps(None)
                        related_manga = json.dumps(None)
                        volumes = i.get('num_volumes')
                        chapters = i.get('num_chapters')
                        season = i.get('start_season', {}).get('season', None)
                        query = f'INSERT INTO {entity} (id, title, cover, alternative_titles, start_date, end_date, synopsis, nsfw, authors, created_at, updated_at, media_type, status, related_anime, related_manga, volumes, chapters, season) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
                        cursor.execute(query, (id, title, cover, alternative_titles, start_date, end_date, synopsis, nsfw, authors, created_at, updated_at, media_type, status, related_anime, related_manga, volumes, chapters, season,))
        
                        # add genres to junction table
                        genres = [genre['name'] for genre in i.get('genres', [])]
                        for genre in genres:
                            query = f'SELECT id FROM {ENTITY.GENRE} WHERE name = %s'
                            cursor.execute(query, (genre,))
                            genre_id = cursor.fetchone()

                            if genre_id is not None:
                                query = f'INSERT INTO {entity}_{ENTITY.GENRE} ({entity}_id, {ENTITY.GENRE}_id) VALUES(%s, %s);'
                                cursor.execute(query, (id, genre_id,))
        elif entity == ENTITY.STATISTICS:
            with psycopg2.connect(**DB_CONFIG) as db:
                db.autocommit = True
                with db.cursor() as cursor:
                    query = f'''
CREATE TABLE IF NOT EXISTS {ENTITY.ANIME}_{entity} (
    {ENTITY.ANIME}_id int not null,
    {entity}_id int not null,
    primary key ({entity}_id, {ENTITY.ANIME}_id),
    foreign key ({ENTITY.ANIME}_id) references {ENTITY.ANIME}(id) on delete cascade,
    foreign key ({entity}_id) references {entity}(id) on delete cascade
);'''
                    cursor.execute(query) # anime_statistics table

                    query = f'''
CREATE TABLE IF NOT EXISTS {ENTITY.MANGA}_{entity} (
    {ENTITY.MANGA}_id int not null,
    {entity}_id int not null,
    primary key ({entity}_id, {ENTITY.MANGA}_id),
    foreign key ({ENTITY.MANGA}_id) references {ENTITY.MANGA}(id) on delete cascade,
    foreign key ({entity}_id) references {entity}(id) on delete cascade
);'''
                    cursor.execute(query) # manga_statistics table

            query = f'INSERT INTO {entity}(mean, rank, popularity, users_listed, users_scored, watching, completed, on_hold, dropped, plan_to_watch) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;'
            with psycopg2.connect(**DB_CONFIG) as db:
                db.autocommit = True
                with db.cursor() as cursor:
                    for entry in animes:
                        id = entry.get('id')
                        mean = entry.get('mean')
                        rank = entry.get('rank')
                        popularity = entry.get('popularity')
                        users_listed = entry.get('num_list_users')
                        users_scored = entry.get('num_scoring_users')
                        watching = entry.get('statistics', None).get('status').get('watching')
                        completed = entry.get('statistics', None).get('status').get('completed')
                        on_hold = entry.get('statistics', None).get('status').get('on_hold')
                        dropped = entry.get('statistics', None).get('status').get('dropped')
                        plan_to_watch = entry.get('statistics', None).get('status').get('plan_to_watch')
                        cursor.execute(query, (mean, rank, popularity, users_listed, users_scored, watching, completed, on_hold, dropped, plan_to_watch,))
                        statistics_id = cursor.fetchone()[0]
                        as_query = f'INSERT INTO {ENTITY.ANIME}_{entity}({ENTITY.ANIME}_id, {entity}_id) VALUES(%s, %s);'
                        cursor.execute(as_query, (id, statistics_id,))
                    for entry in mangas:
                        id = entry.get('id')
                        mean = entry.get('mean')
                        rank = entry.get('rank')
                        popularity = entry.get('popularity')
                        users_listed = entry.get('num_list_users')
                        users_scored = entry.get('num_scoring_users')
                        watching = None
                        completed = None
                        on_hold = None
                        dropped = None
                        plan_to_watch = None
                        cursor.execute(query, (mean, rank, popularity, users_listed, users_scored, watching, completed, on_hold, dropped, plan_to_watch,))
                        statistics_id = cursor.fetchone()[0]
                        ms_query = f'INSERT INTO {ENTITY.MANGA}_{entity}({ENTITY.MANGA}_id, {entity}_id) VALUES(%s, %s);'
                        cursor.execute(ms_query, (id, statistics_id,))
        else:
            print(f'Entity not supported: {entity}')

        time_elapsed = (time.time() - TIME_CHECKPOINT)
        print(f'Executing queries for {entity} took {time_elapsed:.5f}s')
        TIME_CHECKPOINT = time.time()
    
    TIME_END = time.time()
    time_elapsed = (TIME_END - TIME_START)
    print(f'Generating and executing for all queries took {time_elapsed:.5f}s')

def prep():
    print(f'Preparing database...')

    for entity in entities:
        drop_entities(entity)
        create_entities(entity)

def create_entities(entity):
    query = None    
    if entity == ENTITY.GENRE:
        query = f'''
CREATE TABLE IF NOT EXISTS {entity}(
    id int primary key not null,
    name text
);'''
    elif entity == ENTITY.MEDIA_TYPE:
        pass
    elif entity == ENTITY.NSFW:
        query = f'''
CREATE TYPE {entity} AS ENUM(
    'white',
    'gray',
    'black'
);'''
    elif entity == ENTITY.RATING:
        pass
    elif entity == ENTITY.STATUS:
        pass
    elif entity == ENTITY.SOURCE:
        pass
    elif entity == ENTITY.SEASON:
        query = f'''
CREATE TYPE {entity} AS ENUM(
    'winter',
    'spring',
    'summer',
    'fall'
);'''
    elif entity == ENTITY.STUDIO:
        query = f'''
CREATE TABLE IF NOT EXISTS {entity}(
    id int primary key not null,
    name text
);'''
    elif entity == ENTITY.AUTHOR:
        pass
    elif entity == ENTITY.STATISTICS:
        query = f'''
CREATE TABLE IF NOT EXISTS {entity}(
    id serial primary key not null,
    mean decimal,
    rank int,
    popularity int,
    users_listed int,
    users_scored int,
    watching int,
    completed int,
    on_hold int,
    dropped int,
    plan_to_watch int
);'''
    elif entity == ENTITY.ANIME:
        pass
    elif entity == ENTITY.MANGA:
        pass
    else:
        print(f'Entity not supported: {entity}')

    if query is not None:
        print(f'Creating {entity}...')
        with psycopg2.connect(**DB_CONFIG) as db:
            db.autocommit = True
            with db.cursor() as cursor:
                cursor.execute(query)

def drop_entities(entity):
    query = None
    if entity == ENTITY.GENRE:
        query = f'DROP TABLE IF EXISTS {entity} CASCADE;'
        query += f'DROP TABLE IF EXISTS anime_{entity} CASCADE;'
        query += f'DROP TABLE IF EXISTS manga_{entity} CASCADE;'
    elif entity == ENTITY.MEDIA_TYPE:
        query = f'DROP TYPE IF EXISTS {entity} CASCADE;'
    elif entity == ENTITY.NSFW:
        query = f'DROP TYPE IF EXISTS {entity} CASCADE;'
    elif entity == ENTITY.RATING:
        query = f'DROP TYPE IF EXISTS {entity} CASCADE;'
    elif entity == ENTITY.STATUS:
        query = f'DROP TYPE IF EXISTS {entity} CASCADE;'
    elif entity == ENTITY.SOURCE:
        query = f'DROP TYPE IF EXISTS {entity};'
    elif entity == ENTITY.SEASON:
        query = f'DROP TYPE IF EXISTS {entity} CASCADE;'
    elif entity == ENTITY.STUDIO:
        query = f'DROP TABLE IF EXISTS {entity} CASCADE;'
        query += f'DROP TABLE IF EXISTS anime_{entity} CASCADE;'
    elif entity == ENTITY.STATISTICS:
        query = f'DROP TABLE IF EXISTS {entity} CASCADE;'
        query += f'DROP TABLE IF EXISTS anime_{entity} CASCADE;'
        query += f'DROP TABLE IF EXISTS manga_{entity} CASCADE;'
    elif entity == ENTITY.ANIME:
        query = f'DROP TABLE IF EXISTS {entity} CASCADE;'
    elif entity == ENTITY.MANGA:
        query = f'DROP TABLE IF EXISTS {entity} CASCADE;'
    else:
        print(f'Entity not supported: {entity}')

    if query is not None:
        print(f'Dropping {entity} if exists...')
        with psycopg2.connect(**DB_CONFIG) as db:
            db.autocommit = True
            with db.cursor() as cursor:
                cursor.execute(query)

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
