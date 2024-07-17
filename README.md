# MAL Scrapper
This script will get every single anime and manga from [MAL](https://myanimelist.net).  

## Usage
### Command
```
python scrapper.py [-h] [--endpoint {anime,manga}] --mode {scrap,validate,cleaner,seasonal} [--start START] [--limit LIMIT] [--key KEY]
```

### Options 
| Argument | Required | Type | Options | Description | 
| --- | --- | --- | --- | --- |  
| `-h` | False | Flag | | Shows a help message |  
| `--endpoint` | False<br>Default: `anime` | String | `anime`<br>`manga` | Chooses the endpoint. |  
| `--mode` | True | String | `scrap`, `validate`, `cleaner` and `seasonal` | Mode of operation.<br>`scrap` is basically a brute-force method, which will go from `n` to `m` and checks whether its `id` is an valid entry or not.<br>`validate` does the same with the difference that it checks all the missed entries which occurred during a rate limit or other unknown reasons.<br>`cleaner` merges all files into one and will clean the 503 json file. It will remove every duplication as well. |  
| `--start` | False<br>Default: `1` or last entry id + 1 | Integer | --- | Total number of entries to scrap/validate. |  
| `--limit` | False | Integer | --- | Upper limit of entries to scrap/validate. |  
| `--key` | True<br>(False in `cleaner` mode) | String | --- | API key which is needed within the `scrap` and `validate` mode. |  

## Query generator
_todo_