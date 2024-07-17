import requests
import json
import time
import os
import argparse
import re

API_ENDPOINT_ANIME = 'https://api.myanimelist.net/v2/anime'
API_ENDPOINT_MANGA = 'https://api.myanimelist.net/v2/manga'
ENDPOINT_ANIME = 'anime'
ENDPOINT_MANGA = 'manga'
SELECTED_ENDPOINT = ENDPOINT_ANIME
CLIENT_ID = None
RATE_LIMITED_VALIDATE_LIST_ANIME = '503_anime.json'
RATE_LIMITED_VALIDATE_LIST_MANGA = '503_manga.json'
NOT_FOUND_LIST_ANIME = '404_anime.json'
NOT_FOUND_LIST_MANGA = '404_manga.json'
DB_LIST_ANIME = 'db_anime.json'
DB_LIST_MANGA = 'db_manga.json'
REQUEST_START_N = None
REQUEST_LIMIT_N = 70000
SAVE_PROGRESS_N = 5

def get_data(client_id, endpoint, id):
    if endpoint == ENDPOINT_ANIME:
        params = {
            'fields': 'id,title,main_picture,alternative_titles,start_date,end_date,synopsis,mean,rank,popularity,num_list_users,num_scoring_users,nsfw,created_at,updated_at,media_type,status,genres,my_list_status,num_episodes,start_season,broadcast,source,average_episode_duration,rating,pictures,background,related_anime,related_manga,recommendations,studios,statistics'
        }
        api_url = API_ENDPOINT_ANIME
    elif endpoint == ENDPOINT_MANGA:
        params = {
            'fields': 'id,title,main_picture,alternative_titles,start_date,end_date,synopsis,mean,rank,popularity,num_list_users,num_scoring_users,nsfw,created_at,updated_at,media_type,status,genres,my_list_status,num_volumes,num_chapters,authors{first_name,last_name},pictures,background,related_anime,related_manga,recommendations,serialization{name}'
        }
        api_url = API_ENDPOINT_MANGA

    headers = {
        'X-MAL-CLIENT-ID': client_id
    }

    response = requests.get(f"{api_url}/{id}", params=params, headers=headers)

    if response.status_code == 200:
        return response.json()
    elif response.status_code == 404:
        print(f"No {endpoint} found with id {id}")
        if endpoint == ENDPOINT_ANIME:
            entries = load_list(NOT_FOUND_LIST_ANIME)
            entries.append(id)
            save_list(entries, NOT_FOUND_LIST_ANIME)
        elif endpoint == ENDPOINT_MANGA:
            entries = load_list(NOT_FOUND_LIST_MANGA)
            entries.append(id)
            save_list(entries, NOT_FOUND_LIST_MANGA)
    else:
        print(f"Error {response.status_code}: Rate limited for {endpoint} id {id}")
    
    return None

def load_list(filename):
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    return []

def save_list(entries, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(entries, f, ensure_ascii=False, indent=4)

def get_last_id(entries):
    return max(x['id'] for x in entries) if entries else 0

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser()
        
        parser.add_argument(
            '-e',
            '--endpoint', 
            choices=['anime', 'manga'], 
            default=ENDPOINT_ANIME, 
            help='Choose between the anime or manga endpoint.'
        )

        parser.add_argument(
            '-m',
            '--mode', 
            choices=['scrap', 'validate', 'cleaner', 'seasonal'], 
            required=True, 
            help='Mode of operation. Choose from: scrap, validate, cleaner, seasonal'
        )

        parser.add_argument(
            '-s',
            '--start',
            type=int,
            default=REQUEST_START_N,
            help=f'Total number of entries to scrap/validate. Default is {REQUEST_START_N}'
        )

        parser.add_argument(
            '-l',
            '--limit',
            type=int,
            default=REQUEST_LIMIT_N,
            help=f'Upper limit of entries to scrap/validate. Default is {REQUEST_LIMIT_N}'
        )

        parser.add_argument(
            '-k',
            '--key',
            type=str,
            default=CLIENT_ID,
            help='API key which is needed within the {scrap,validate,seasonal} mode.'
        )

        args = parser.parse_args()
        SELECTED_ENDPOINT = args.endpoint
        REQUEST_START_N = args.start
        REQUEST_LIMIT_N = args.limit
        CLIENT_ID = args.key

        if args.mode != 'cleaner' and CLIENT_ID is None:
            print(f'== Please provide an api key (--key <client_id>) to use the {args.mode} mode. ==')
            exit(0)

        if SELECTED_ENDPOINT == ENDPOINT_ANIME:
            api_endpoint_url = API_ENDPOINT_ANIME
            db = load_list(DB_LIST_ANIME)
            not_found_list = load_list(NOT_FOUND_LIST_ANIME)
            rate_limited_validate_list = load_list(RATE_LIMITED_VALIDATE_LIST_ANIME)
            filename_rate_limited_validate_list = RATE_LIMITED_VALIDATE_LIST_ANIME
            filename_db = DB_LIST_ANIME
        elif SELECTED_ENDPOINT == ENDPOINT_MANGA:
            api_endpoint_url = API_ENDPOINT_MANGA
            db = load_list(DB_LIST_MANGA)
            not_found_list = load_list(NOT_FOUND_LIST_MANGA)
            rate_limited_validate_list = load_list(RATE_LIMITED_VALIDATE_LIST_MANGA)
            filename_rate_limited_validate_list = RATE_LIMITED_VALIDATE_LIST_MANGA
            filename_db = DB_LIST_MANGA
        
        time_start = time.time()
        new_entry_count = 0

        if args.mode == 'scrap':
            if REQUEST_START_N is None:
                REQUEST_START_N = get_last_id(db) + 1
            if REQUEST_LIMIT_N < REQUEST_START_N:
                print('Request limit is smaller than the start...')
            
            print(f"Loaded {len(db)} existing {SELECTED_ENDPOINT} entries")
            print(f"Starting from ID: {REQUEST_START_N}")
            
            for id in range(REQUEST_START_N, REQUEST_LIMIT_N + 1):
                entry = get_data(CLIENT_ID, SELECTED_ENDPOINT, id)
                if entry:
                    if len(db) == 0:
                        db = []

                    db.append(entry)
                    new_entry_count += 1
                    print(f"Added {SELECTED_ENDPOINT} with id {id}. [Total new entries: {new_entry_count}]")

                    if new_entry_count % SAVE_PROGRESS_N == 0:
                        save_list(db, filename_db)
                        time_mid = time.time()
                        time_elapsed = (time_mid - time_start)
                        print(f"Saved progress! Total {SELECTED_ENDPOINT}s saved: {len(db)}\nTime eplased in this session: {time_elapsed}s")

                if new_entry_count % 100 == 0 and new_entry_count > 0:
                    print("Sleeping for 90 seconds...")
                    time.sleep(90)
                else:
                    time.sleep(1)  

            save_list(db, filename_db)
            print(f"Added {new_entry_count} new {SELECTED_ENDPOINT} entries.\nTotal {SELECTED_ENDPOINT}s in {filename_db}: {len(db)}")
        elif args.mode == 'validate':
            used_ids = []
            unused_ids = []
            print(f'Getting ids which appear in {filename_db} and {filename_rate_limited_validate_list}')
            for i in db:
                used_ids.append(i.get('id'))

            for i in rate_limited_validate_list:
                used_ids.append(i.get('id'))

            print(f'Getting free ids which not appear in {filename_db} and {filename_rate_limited_validate_list}')
            for i in range(1, REQUEST_LIMIT_N):
                if i not in used_ids and i not in not_found_list:
                    unused_ids.append(i)

            print('Trying to scrap the possible unused ids...')
            for i in unused_ids:
                entry = get_data(CLIENT_ID, SELECTED_ENDPOINT, i)
                if entry:
                    if len(rate_limited_validate_list) == 0:
                        rate_limited_validate_list = []

                    rate_limited_validate_list.append(entry)
                    new_entry_count += 1
                    print(f"Added {SELECTED_ENDPOINT} with id {i}. [Total new entries: {new_entry_count}]")

                    if new_entry_count % SAVE_PROGRESS_N == 0:
                        save_list(rate_limited_validate_list, filename_rate_limited_validate_list)
                        time_mid = time.time()
                        time_elapsed = (time_mid - time_start)
                        print(f"Saved progress! Total {SELECTED_ENDPOINT}s saved: {len(rate_limited_validate_list)}\nTime eplased in this session: {time_elapsed}s")

                if new_entry_count % 100 == 0 and new_entry_count > 0:
                    print("Sleeping for 90 seconds...")
                    time.sleep(90)
                else:
                    time.sleep(1)  
            
            save_list(rate_limited_validate_list, filename_rate_limited_validate_list)
            print(f"Added {new_entry_count} new {SELECTED_ENDPOINT} entries.\nTotal {SELECTED_ENDPOINT}s in {filename_db}: {len(rate_limited_validate_list)}")
        elif args.mode == 'cleaner':
            merged_list = db + rate_limited_validate_list
            seen_ids = set()
            cleaned_list = [entry for entry in merged_list if entry['id'] not in seen_ids and not seen_ids.add(entry['id'])]
            db = sorted(cleaned_list, key=lambda x: x['id'])
            save_list(db, filename_db)
            save_list([], filename_rate_limited_validate_list)
            print(f'Sorted and cleaned {len(db)} {SELECTED_ENDPOINT}s...')
        elif args.mode == 'seasonal':
            params = {
                    'limit': 100
                }
            headers = {
                'X-MAL-CLIENT-ID': CLIENT_ID
            }

            year = 2025
            season = 'summer'
            season_anime_list = []

            while True:
                response = requests.get(f"https://api.myanimelist.net/v2/anime/season/{year}/{season}", params=params, headers=headers)

                if response.status_code == 200:
                    data = response.json()
                    season_anime_list.append(data['data'])
                    save_list(season_anime_list, "current.json")
                    if data['paging'].get('next'):
                        paging = data.get('paging')
                        if paging:
                            if paging.get('next'):
                                offset = re.search(r'offset=(\d*)', paging.get('next'))
                                params = {
                                    'limit': 100,
                                    'offset': offset.group(1)
                                }
                    else:
                        print(f'No more season animes for {season} {year}')
                        break
                else:
                    print(f'Error {response.status_code}')
                    break

    except KeyboardInterrupt:
        print('\nSaving current progress before exiting...')
        merged_list = db + rate_limited_validate_list
        seen_ids = set()
        cleaned_list = [entry for entry in merged_list if entry['id'] not in seen_ids and not seen_ids.add(entry['id'])]
        db = sorted(cleaned_list, key=lambda x: x['id'])
        save_list(db, filename_db)
        save_list(rate_limited_validate_list, filename_rate_limited_validate_list)
        print(f'Saved {len(db)} {SELECTED_ENDPOINT}s in {filename_db}')
        print(f'Saved {len(rate_limited_validate_list)} {SELECTED_ENDPOINT}s in {filename_rate_limited_validate_list}')
        print('Exiting scrapper...')
