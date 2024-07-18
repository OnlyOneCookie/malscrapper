import os
import time
import argparse

from enum import Enum

from utils import scrapper, query

API_ENDPOINT_ANIME = 'https://api.myanimelist.net/v2/anime'
API_ENDPOINT_MANGA = 'https://api.myanimelist.net/v2/manga'
ENDPOINT_ANIME = 'anime'
ENDPOINT_MANGA = 'manga'
SELECTED_ENDPOINT = ENDPOINT_ANIME
SELECTED_MODE = None
CLIENT_ID = None
REQUEST_START_N = None
REQUEST_LIMIT_N = 60000
SAVE_PROGRESS_N = 1
ROOT_FOLDER = os.path.dirname(
    os.path.abspath(__file__)
)
EXPORT_FOLDER_QUERIES = f'{ROOT_FOLDER}/queries'
EXPORT_FOLDER_DATA = f'{ROOT_FOLDER}/data'
RATE_LIMITED_VALIDATE_LIST_ANIME = f'{EXPORT_FOLDER_DATA}/503_anime.json'
RATE_LIMITED_VALIDATE_LIST_MANGA = f'{EXPORT_FOLDER_DATA}/503_manga.json'
NOT_FOUND_LIST_ANIME = f'{EXPORT_FOLDER_DATA}/404_anime.json'
NOT_FOUND_LIST_MANGA = f'{EXPORT_FOLDER_DATA}/404_manga.json'
DB_LIST_ANIME = f'{EXPORT_FOLDER_DATA}/db_anime.json'
DB_LIST_MANGA = f'{EXPORT_FOLDER_DATA}/db_manga.json'

class MODE(Enum):
    SCRAP = 'scrap'
    VALIDATE = 'validate'
    CLEANUP = 'cleanup'
    QUERY = 'query'

    def __repr__(self):
        return self.value

    def __str__(self):
        return self.value
    
    def __eq__(self, other):
        if isinstance(other, MODE):
            return self.value == other.value
        if isinstance(other, str):
            return self.value == other
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser()

        parser.add_argument(
            '-m',
            '--mode', 
            choices=[MODE.SCRAP.value, MODE.VALIDATE.value, MODE.CLEANUP.value, MODE.QUERY.value], 
            required=True, 
            help=f'Mode of operation. Choose from: {MODE.SCRAP}, {MODE.VALIDATE}, {MODE.CLEANUP}, {MODE.QUERY}'
        )
    
        parser.add_argument(
            '-e',
            '--endpoint', 
            choices=['anime', 'manga'], 
            default=ENDPOINT_ANIME, 
            help='Choose between the anime or manga endpoint.'
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
        SELECTED_MODE = args.mode
        SELECTED_ENDPOINT = args.endpoint
        REQUEST_START_N = args.start
        REQUEST_LIMIT_N = args.limit
        CLIENT_ID = args.key

        if SELECTED_MODE != MODE.CLEANUP and SELECTED_MODE != MODE.QUERY and CLIENT_ID is None:
            print(f'== Please provide an api key (--key <client_id>) to use the {SELECTED_MODE} mode. ==')
            exit(0)

        if SELECTED_MODE == MODE.QUERY:
            query.generator()
        else:
            if SELECTED_ENDPOINT == ENDPOINT_ANIME:
                api_endpoint_url = API_ENDPOINT_ANIME
                db = scrapper.load_list(DB_LIST_ANIME)
                not_found_list = scrapper.load_list(NOT_FOUND_LIST_ANIME)
                rate_limited_validate_list = scrapper.load_list(RATE_LIMITED_VALIDATE_LIST_ANIME)
                filename_db = DB_LIST_ANIME
                filename_not_found = NOT_FOUND_LIST_ANIME
                filename_rate_limited_validate_list = RATE_LIMITED_VALIDATE_LIST_ANIME
            elif SELECTED_ENDPOINT == ENDPOINT_MANGA:
                api_endpoint_url = API_ENDPOINT_MANGA
                db = scrapper.load_list(DB_LIST_MANGA)
                not_found_list = scrapper.load_list(NOT_FOUND_LIST_MANGA)
                rate_limited_validate_list = scrapper.load_list(RATE_LIMITED_VALIDATE_LIST_MANGA)
                filename_db = DB_LIST_MANGA
                filename_not_found = NOT_FOUND_LIST_MANGA
                filename_rate_limited_validate_list = RATE_LIMITED_VALIDATE_LIST_MANGA
                
        
    except KeyboardInterrupt:
        pass