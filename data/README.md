## Data

This directory contains the raw data used to populate the data model as well as output data corresponding with the model saved in Parquet format. A data dictionary corresponding with the tables in the data model is provided below.

**Note:** All data have been gitignored from this repo.

Details on the data source can be found at `/data_access/README.md`

## Data Dictionary

### Dimensional Tables

#### AtlasDetails

AtlasDetails provides data on board games in the Board Game Atlas database and includes:

- **atlas_game_id (PK)**: unique identifier for a board game from Board Game Atlas (matches the unique identifier from the Board Game Atlas API)
- **game_name**: board game name
- **url**: url of the board game for the Board Game Atlas website
- **datetime_extracted**: datetime the board game data was extracted from Board Game Atlas (UTC)
- **year_published**: year that the board game was published
- **min_players**: minimum players for the board game
- **max_players**: maximum players for the board game
- **min_playtime**: minimum estimated playtime for the board game
- **max_playtime**: maximum estimated playtime for the board game
- **min_age**: minimum recommended age for the board game
- **primary_publisher**: primary publisher of the board game (there may be multiple publishers)
- **primary_designer**: pimary designer of the board game (there may be multiple designers)
- **artists**: string list of artists that contributed to the board game
- **num_ratings**: number of ratings by Board Game Atlas users that the board game has received
- **average_rating**: average rating of the board game made by Board Game Atlas users
- **num_complexity**: number of game complexity ratings by Board Game Atlas users that the board game has received
- **average_learning_complexity**: average learning complexity rating of the board game made by Board Game Atlas users
- **average_strategy_complexity**: average strategy complexity rating of the board game made by Board Game Atlas users

#### AtlasUsers

AtlasUsers provides data on Board Game Atlas users and includes:

- **user_id (PK)**: unique identifier for a user from Board Game Atlas (does not match the unique identifier from the Board Game Atlas API)
- **atlas_user_id**: unique identifier for a user from Board Game Atlas matching the Board Game Atlas identifier - was not used as the PK for the table as this field was unable to be extracted for every user
- **user_name**: Board Game Atlas user name
- **url**: url of the user for the Board Game Atlas website
- **description**: Board Game Atlas self user entered profile description
- **atlas_gold**: Board Game Atlas user gold which relates to experience
- **atlas_exp**: Board Game Atlas user experience (point based system)
- **atlas_level**: Board Game Atlas user level which is based on the amount of experience
- **atlas_premium**: if the user is has a Board Game Atlas premium account
- **atlas_partner**: if the user is a Board Game Atlas partner
- **atlas_moderator**: if the user is a moderator on Board Game Atlas

#### BGGDetails

BGGDetails provides data on board games in the Board Game Atlas database and includes:

- **bgg_game_id (PK)**: unique identifier for a board game from BoardGameGeek (matches the unique identifier from the BoardGameGeek API)
- **game_name**: board game name
- **datetime_extracted**: datetime the board game data was extracted from BoardGameGeek (UTC)
- **min_players**: minimum players for the board game
- **max_players**: maximum players for the board game
- **min_playtime**: minimum estimated playtime for the board game
- **max_playtime**: maximum estimated playtime for the board game
- **min_age**: minimum recommended age for the board game
- **year_published**: year that the board game was published
- **publisher**: string list of publishers of the board game (there may be multiple publishers)
- **designer**: string list of designer of the board game (there may be multiple designers)
- **artists**: string list of artists that contributed to the board game
- **category**: string list of categories the board game belongs to (examples: Card Game, Fantasy)
- **compilation**: string list of compilations the board game belongs to (example: The Board Game Book)
- **family**: string list of families the board game belongs to (example: Animals: Mice, Religious: The Bible)
- **mechanic**: string list of mechanics the board game belongs to (example: Alliances, Trick-Taking)
- **num_ratings**: number of ratings by BoardGameGeek users that the board game has received
- **average_rating**: average rating of the board game made by BoardGameGeek users

#### Time

The time table provides a breakdown of time units associated with a datetime, the fields include: datetime, minute, hour, day, week, month, year, weekday.

### Fact Tables

#### AtlasReviews

AtlasReviews includes board game reviews from Board Game Atlas and can be used to assess game popularity trends over time. Fields include:

- **review_id (PK)**: unique identifier for a review made on Board Game Atlas (matches the Board Game Atlas review id from the API)
- **review_datetime**: datetime (UTC) when the review was made, used to join the dimension table Time
- **user_id (not null)**: id used to join the dimension table AtlasUsers
- **game_name**: board game name
- **atlas_game_id (not null)**: id used to join the dimension table AtlasDetails
- **bgg_game_id**: id used to join the dimension table BGGDetails
- **rating (not null)**: review for the board game made on Board Game Atlas (scale of 0-5 in 0.25 increments)
- **description**: description for the review of the board game made on Board Game Atlas

#### AtlasPrices

AtlasPrices includes board game prices for games over time from Board Game Atlas and can be used to assess board game price changes over time, or query the best price that has existed for a game. The prices are scraped from various online stores. Fields include:

- **atlas_price_id (PK)**: unique identifier for a price available on Board Game Atlas (matches the Board Game Atlas price id from the API)
- **price_datetime (not null)**: datetime (UTC) when Board Game Atlas scraped the price, used to join the dimension table Time
- **atlas_game_id (not null)**: id used to join the dimension table AtlasDetails
- **game_name**: board game name (however, this is the description from the item scraped by Board Game Geek and may includes more than just the board game name - this is the reason that not all Board Game Atlas and BoardGameGeek ids were able to be associated with the game name)
- **bgg_game_id**: id used to join the dimension table BGGDetails
- **price**: price of the board game scraped by Board Game Atlas
- **currency**: currency for the price of the board game scraped by Board Game Atlas
- **msrp**: msrp of the board game (unknown how this is generated)
- **url**: url corresponding with the board game on the online store
- **store_name**: store name of the online store
- **country**: country that the online store is associated with
- **price_category**: Board Game Atlas divides their prices between "au", "canada", "uk", "us", and "used" - it is unclear exactly what this specifies but has been included in the fact table

#### BGGLists

BGGLists includes board game blog lists from BoardGameGeek that could be used to assess game popularity. Each row in the fact table corresponds with a game included in a BoardGameGeek list. Fields include:

- **list_item_id (PK)**: unique identifier for the table
- **geeklist_id (not null)**: id of the BoardGameGeek GeekList (matches the BoardGameGeek API)
- **post_datetime (not null)**: datetime (UTC) for the GeekList post, used to join the dimension table Time
- **objtype**: the type of object associated with the name mentioned in the GeekList (typically "board game")
- **bgg_game_id (not null)**: id used to join the dimension table BGGDetails
- **game_name**: board game name
- **atlas_game_id**: id used to join the dimension table AtlasDetails
- **bgg_user_name**: BoardGameGeek user name that made the GeekList post
- **description**: description of the item made in the GeekList post
