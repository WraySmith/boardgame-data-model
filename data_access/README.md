# Dataset Generation

The data used in this project is from the two sources noted below. There are approximately **1.136M** rows of data.

Scripts used to compile the data from the APIs can be found in this directory. The resulting data is available at `/data/` and data dictionaries for data used in the model can be found at `/data/README.md`.

The scripts provided in this directory were created to create the dataset to support the Board Game Dataset Model Project. The scripts are currently not considered as part of the model as they are not sufficiently at a point where they could be run on an automated of semi-automated basis as:

- The time to query the APIs to generate the dataset is in the order of days for multiple of the datasets discussed in the following sections.
- Accordingly, to include the API queries as part of the data model, it would likely require writing the scripts to check data already available in the dataset and only query new data. This was considered beyond the scope of this Project and would take considerable effort (however, it would be an obvious next step if this was being used in production).

As such, the scripts below are only included in this repo to provide an understanding of how the raw dataset was generated for the Project.

## [BoardGameGeek (BGG) API](https://boardgamegeek.com/wiki/page/BGG_XML_API)

The API provides XML output which is then compiled into csv format. Endpoints accessed include:

- `https://boardgamegeek.com/xmlapi/boardgame/` provides information about a particular boardgame - saved to `/data/raw/bgg/games.csv`.
- `https://boardgamegeek.com/xmlapi/geeklist/` which provides GeekList information - saved to `/data/raw/bgg/lists.csv`.

Challenges of using the BoardGameGeek API include:

- A query can only be made approximately every 5 seconds based on guidance docs and that two queries are sometimes required to access the geeklist endpoint (one query to put the request in the queue and the second to access the resuls).
- The XML output is not consistent (appears to be historical variances).
- The API sometimes returns `ChunkEncodingError` and requires requerying.

### Scripts

`bgg_api.py` provides the following functions that can be used to query the API and compile data:

- `query_bgg_api()` - query the API and return XML data
- `get_games()` - retrive game data from the API and clean it into data rows
- `get_geeklist()` - retrieve GeekList data from the API and clean it into data rows
- `compile_data()` - copmiles a dataframe of data based on a list of games or geeklists

### Data Retrieved

 - All games available from the BoardGameGeek API were queried resulting in ~343k games.
 - GeekList ids from 290,000 onwards were queried from the API resuling in ~351 games noted in GeekLists.

## [Board Game Atlas](https://www.boardgameatlas.com/)

The API provides JSON output which is compiled into JSON format. Endpoints accessed include:

- `https://www.boardgameatlas.com/api/docs/prices` provides price data for a game - saved to `/data/raw/atlas/prices/`.
- `https://www.boardgameatlas.com/api/docs/users` provides a list of all Board Game Atlas users with information - saved to `/data/raw/atlas/json_user.json`.
- `https://www.boardgameatlas.com/api/docs/reviews` provides game reviews as well as additional information on the user that made the review - saved to `/data/raw/atlas/reviews/` and `/data/raw/atlas/user/`, respectively.
- `https://www.boardgameatlas.com/api/docs/search` provides board game details - saved to `/data/raw/atlas/games/`

Challenges of the using the Board Game Atlas API include:

- Several of the endpoint API parameters require knowing the ID of the user or game. Therefore, data needs to be extracted from one of the endpoints and compiled and then used to query another endpoint. For example, the game ID needs to be known to be able to query for game prices. However, to get the game ID, one needs to compile the set of available game IDs from the user reviews.
- Several endpoints limit the number of entries that can be returned but also limit the number of entries that can be skipped making it difficult to retrieve all data. Using parameters to filter entries in the API queries were required in these instances.

### Scripts

`atlas_api.py` provides the following functions that can be used to query the API and compile data:

- `query_atlas_api()` - query the API and return JSON data
- `compile_users()` - compile the list of Board Game Atlas users
- `compile_reviews()` - compiles available Board Game Atlas reviews
    - requires the user list to be compiled from `compile_users()`
- `clean_reviews()` - removes unecessary data from Board Game Atlas reviews and extracts additional user data
- `compile_games()` - compiles Board Game Atlas game data
    - requires the reviess data to be compiled from `compile_reviews()`
- `compile_prices()` - compiles Board Game Atlas game price data
    - requires the reviess data to be compiled from `compile_reviews()`
- `combine_files()` - combines JSON files to reduce the volume of files

### Data Retrieved

 - All users from the Board Game Atlas API were queried resulting in ~5k users.
 - All reviews from the API were queried resulting in ~227k reviews.
 - Games corresponding with games noted in reviews were queried from the API resulting in 23.5k games.
 - Prices corresponding with games noted in reviews were queried from the API resulting in ~187k prices.