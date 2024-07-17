import requests
import json
import time
import re
import os
import argparse
import pathlib
import psycopg2

from enum import Enum

ENDPOINT_ANIME = 'anime'
ENDPOINT_MANGA = 'manga'
SELECTED_ENDPOINT = None
EXPORT_FOLDER = ''

db_meta = {
    "host": "localhost",
    "database": "sololeveling",
    "user": "solo",
    "password": "leveling"
}

class TABLES(Enum):
    ANIME = 'anime'
    MANGA = 'manga'
    NSFW = 'nsfw'
    MEDIA_TYPE = 'media_type'
    GENRE = 'genre'
    STATUS = 'status'
    SOURCE = 'source'
    SEASON = 'season'
    STUDIO = 'studio'
    STATISTICS = 'statistics'
    RATING = 'rating'

tables = TABLES

def send_query(query):
    db = psycopg2.connect(**db_meta)
    cur = db.cursor()
    cur.execute(query)
    db.close()

def query_generator():
    data = get_data(f'db_{SELECTED_ENDPOINT}.json')
    print(f'Generating queries with {len(data)} anime entries...')
    for table in tables:
        print(f'Creating query for {table.value}...')

        if table.value == TABLES.MANGA.value:
            query = f'''
CREATE TABLE IF NOT EXISTS {table.value}(
    id int primary key not null,
    title json
'''

        
        if table.value == TABLES.ANIME.value:
            query = f'''
CREATE TABLE IF NOT EXISTS {table.value}(
    id int primary key not null, 
    title text,
    cover text,
    alternative_titles text[],
    start_date date,
    end_date date,
    genre_ids int[],
    synopsis text,
    nsfw nsfw,
    created_at timestamptz,
    updated_at timestamptz,
    media_type media_type,
    rating rating,
    studio_ids int[],
    status status,
    episodes_count int,
    season season
);
                '''
            
            inserts = []
            for anime in data:
                id = anime['id']
                title_original = f"{anime.get('title')}".replace("'", "''")
                title_en = f"{anime.get('alternative_titles', '').get('en', None)}".replace('"', "''")
                title_jp = f"{anime.get('alternative_titles', '').get('ja', None)}".replace('\x90', '')
                title = [{'original': title_original}, {'en': title_en}, {'jp': title_jp}]
                title = f'{title}'.replace("'", '"').replace('""', "''")
                cover = anime.get('main_picture', {}).get('large', None)
                alternative_titles = {synonym for synonym in anime.get('alternative_titles').get('synonyms', None)}
                alternative_titles = f'{alternative_titles}'.replace("'", "''")
                start_date = anime.get('start_date')
                end_date = anime.get('end_date')
                genres = {genre['id'] for genre in anime.get('genres', [])}
                #synopsis = anime.get('synopsis')
                nsfw = anime.get('nsfw')
                created_at = anime.get('created_at')
                updated_at = anime.get('updated_at')
                media_type = anime.get('media_type')
                rating = anime.get('rating')
                studios = {studio['id'] for studio in anime.get('studios', [])}
                status = anime.get('status')
                episodes_count = anime.get('num_episodes')
                season = anime.get('start_season', {}).get('season', None)
                inserts.append(f'INSERT INTO {table.value} (id, title, cover, alternative_titles, start_date, end_date, genre_ids, nsfw, rating, studio_ids, created_at, updated_at, media_type, status, episodes_count, season) VALUES ({id}, \'{title}\', \'{cover}\', \'{alternative_titles}\', \'{start_date}\', \'{end_date}\', \'{genres}\', \'{nsfw}\', \'{rating}\', \'{studios}\', \'{created_at}\', \'{updated_at}\', \'{media_type}\', \'{status}\', {episodes_count}, \'{season}\');'.replace("'None'", 'null').replace('None', 'null').replace('set()', '{}'))

            for insert in inserts:

                query += f'\n{insert}'
            

            send_query(query)
            export_query(f'{table.value}.sql', query)
            
        if table.value == TABLES.NSFW.value:
            query = f'''
CREATE TYPE {table.value} AS ENUM(
    'white',
    'gray',
    'black'
);
                '''
            
            export_query(f'{table.value}.sql', query)
            
        if table.value == TABLES.MEDIA_TYPE.value:
            media_types = []
            for i in data:
                media_type = i.get('media_type')
                if media_type:
                    media_types.append(media_type)
            
            media_types = list(set(media_types))
            media_types_str = ', '.join(f'\'{media_type}\'' for media_type in media_types)


            query = f'''
CREATE TYPE {table.value} AS ENUM(
    {media_types_str}
);
                '''
            
            export_query(f'{table.value}.sql', query)
            
        if table.value == TABLES.STATUS.value:
            query = f'''
CREATE TYPE {table.value} AS ENUM(
    'not_yet_aired',
    'currently_airing',
    'finished_airing'
);
                '''
            
            export_query(f'{table.value}.sql', query)
            
        if table.value == TABLES.GENRE.value:
            genres = []
            for i in data:
                genres_deep = i.get('genres', [])
                for genre in genres_deep:
                    genres.append(genre)
            
            seen = set()
            genres_finalized = sorted([x for x in genres if not (tuple(x.items()) in seen or seen.add(tuple(x.items())))], key=lambda x: x['id'])

            query = f'''
CREATE TABLE IF NOT EXISTS {table.value}(
    id int primary key not null,
    name text
);
                '''
            
            inserts = []
            for genre in genres_finalized:
                id = genre['id']
                name = genre['name'].replace("'", "''")
                inserts.append(f'INSERT INTO {table.value} (id, name) VALUES ({id}, \'{name}\');')

            for insert in inserts:

                query += f'\n{insert}'

            export_query(f'{table.value}.sql', query)


        if table.value == TABLES.STUDIO.value:
            studios = []
            for i in data:
                studios_deep = i.get('studios', [])
                for studio in studios_deep:
                    studios.append(studio)
            
            seen = set()
            studios_finalized = sorted([x for x in studios if not (tuple(x.items()) in seen or seen.add(tuple(x.items())))], key=lambda x: x['id'])

            query = f'''
CREATE TABLE IF NOT EXISTS {table.value}(
    id int primary key not null,
    name text
);
                '''
            
            inserts = []
            for studio in studios_finalized:
                id = studio['id']
                name = f'{studio["name"]}'.replace("'", "''")
                inserts.append(f'INSERT INTO {table.value} (id, name) VALUES ({id}, \'{name}\');')

            for insert in inserts:

                query += f'\n{insert}'

            export_query(f'{table.value}.sql', query)
            
            
        if table.value == TABLES.SEASON.value:
            query = f'''
CREATE TYPE {table.value} AS ENUM(
    'winter',
    'spring',
    'summer',
    'fall'
);
                '''
            
            export_query(f'{table.value}.sql', query)
            
        if table.value == TABLES.SOURCE.value:
            sources = []
            for i in data:
                source = i.get('source')
                if source:
                    sources.append(source)
            
            sources = list(set(sources))
            sources_str = ', '.join(f'\'{source}\'' for source in sources)


            query = f'''
CREATE TYPE {table.value} AS ENUM(
    {sources_str}
);
                '''
            
            export_query(f'{table.value}.sql', query)
            
        if table.value == TABLES.RATING.value:
            ratings = []
            for i in data:
                rating = i.get('rating')
                if rating:
                    ratings.append(rating)
                
            ratings = list(set(ratings))
            ratings_str = ', '.join(f'\'{rating}\'' for rating in ratings)

            query = f'''
CREATE TYPE {table.value} AS ENUM(
    {ratings_str}
);
                '''
            
            export_query(f'{table.value}.sql', query)

        if table.value == TABLES.STATISTICS.value:
            
#             mn_query = f'''
# CREATE TABLE IF NOT EXISTS {TABLES.STATISTICS.value}(
#     anime_id int references anime (id) on update cascade on delete cascade,
#     statistics_id int references anime (id) on update cascade on delete cascade
# );
# '''
            query = f'''
CREATE TABLE IF NOT EXISTS {TABLES.ANIME.value}_{TABLES.STATISTICS.value}(
    anime_id serial primary key not null,
    watching int,
    completed int,
    on_hold int,
    plan_to_watch int,
    mean decimal,
    rank int,
    popularity int,
    users_listed int,
    users_scored int
);
                '''
            
            inserts = []
            mn_inserts = []
            data_count = 1
            for anime in data:
                anime_id = anime.get('id')
                mean = anime.get('mean')
                rank = anime.get('rank')
                popularity = anime.get('popularity')
                users_listed = anime.get('num_list_users')
                users_scored = anime.get('num_scoring_users')
                watching = anime.get('statistics', None).get('status').get('watching')
                completed = anime.get('statistics', None).get('status').get('completed')
                on_hold = anime.get('statistics', None).get('status').get('on_hold')
                plan_to_watch = anime.get('statistics', None).get('status').get('plan_to_watch')
                inserts.append(f'INSERT INTO {TABLES.ANIME.value}_{TABLES.STATISTICS.value} (anime_id, mean, rank, popularity, users_listed, users_scored, watching, completed, on_hold, plan_to_watch) VALUES({id}, {mean}, {rank}, {popularity}, {users_listed}, {users_scored}, {watching}, {completed}, {on_hold}, {plan_to_watch});'.replace("'None'", 'null').replace('None', 'null'))
#                 mn_inserts.append(f'INSERT INTO {TABLES.ANIME.value}_{TABLES.STATISTICS.value} (anime_id, statistics_id) VALUES ({anime_id}, {data_count});')
                data_count += 1

            for insert in inserts:
                query += f'\n{insert}'
            
#             for insert in mn_inserts:
#                 mn_query += f'\n{insert}'

            export_query(f'{TABLES.ANIME.value}_{TABLES.STATISTICS.value}.sql', query)
#             export_query(f'{TABLES.ANIME.value}_{TABLES.STATISTICS.value}.sql', mn_query)




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
    
def export_query(filename, query):
    with open(f'{EXPORT_FOLDER}/{filename}', 'w+') as file:
        file.write(query)

def query_merger():
    # List of SQL files to merge
    sql_files = [
        f'{TABLES.GENRE.value}.sql', 
        f'{TABLES.MEDIA_TYPE.value}.sql', 
        f'{TABLES.NSFW.value}.sql', 
        f'{TABLES.RATING.value}.sql', 
        f'{TABLES.SEASON.value}.sql', 
        f'{TABLES.SOURCE.value}.sql', 
        f'{TABLES.STATUS.value}.sql', 
        f'{TABLES.STUDIO.value}.sql', 
        f'{TABLES.STATISTICS.value}.sql',
        f'{TABLES.ANIME.value}.sql',
        f'{TABLES.ANIME.value}_{TABLES.STATISTICS.value}.sql'
    ] 

    # Name of the output file
    
    output_file = f'{EXPORT_FOLDER}/_merged.sql'
    
    # Open the output file in write mode
    with open(output_file, 'w') as outfile:
        for sql_file in sql_files:
            filepath = f'{EXPORT_FOLDER}/{sql_file}'
            # Check if the file exists
            if os.path.exists(filepath):
                # Open each SQL file in read mode
                with open(filepath, 'r') as infile:
                    # Read the contents of the SQL file
                    contents = infile.read()
                    # Write the contents to the output file
                    outfile.write(contents)
                    # Optionally add a newline to separate the contents of different files
                    outfile.write('\n\n')
            else:
                print(f"File {sql_file} does not exist.")

    print(f"All SQL files have been merged into {output_file}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-e',
        '--endpoint',
        choices=['anime', 'manga'],
        default=ENDPOINT_ANIME,
        help='Choose between the anime or manga endpoint'
    )

    args = parser.parse_args()

    SELECTED_ENDPOINT = args.endpoint 
    EXPORT_FOLDER = f'../queries/{SELECTED_ENDPOINT}'

    os.makedirs(EXPORT_FOLDER, exist_ok=True)

    query_generator()
    query_merger()
