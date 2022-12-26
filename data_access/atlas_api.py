from datetime import datetime, timezone
import dateutil.parser
import glob
import json
import os
from pathlib import Path
import pytz
import requests
import string
import time
from tqdm import tqdm

import numpy as np

import utils


def query_atlas_api(url, params):
    """
    Queries the Board Game Atlas API and returns
    a json response if available.

    Args:
        url (str): API url to query
        params (dict): dict of params corresponding
            to the api

    Returns:
        xml tree (if no data, returns None)
    """
    back_off = [1, 2, 10, 50, 100, 200, 300, 500, 1000, 2000]
    for sleep_time in back_off:
        print(f"sleeping: {sleep_time}")
        time.sleep(sleep_time)
        try:
            resp = requests.get(url, params=params)
            if resp.status_code == 200:
                data = json.loads(resp.content)
                return data
        except requests.exceptions.ChunkedEncodingError as e:
            print(e)
            print(f"Invalid chunk encoding for {params}")

        except Exception as e:
            print(e)
            print(f"Un-handled error for {params}")

    print("200 code not achieved")
    return


def compile_users(client_id, output_file):
    """
    Compiles user data queried from the Board Game Atlas
    API and saves all users to an output json file.

    Args:
        client_id (str): client id associated with the Board Game
            Atlas API
        output_file (str): output json filepath/name

    Returns:
        None. Saves data to a json file.
    """

    skip = 0
    url = "https://api.boardgameatlas.com/api/users"
    params = {"pretty": "true", "skip": str(skip), "client_id": client_id}

    data = []
    while True:
        temp_data = query_atlas_api(url, params)
        user_data = temp_data["users"]
        if not user_data:
            break
        data.extend(user_data)
        skip += 100
        params["skip"] = str(skip)

    # clean-up
    new_data = []
    for val in data:
        if "username" in val:
            new_data.append(val)

    with open(output_file, "w") as f:
        json.dump(new_data, f)


def compile_reviews(client_id, user_file, output_dir):
    """
    Compiles review data queried from the Board Game Atlas
    API and saves reviews from each user in a json file with the
    json file name corresponding to the Board Game Atlas user id.
    Note, if a user has more than 900 reviews, the API is not able
    to retrieve those reviews.

    Args:
        client_id (str): client id associated with the Board Game
            Atlas API
        user_file (str): user json file populated from `compile_users()`
        output_file (str): output directory

    Returns:
        None. Saves data to json files.
    """

    url = "https://api.boardgameatlas.com/api/reviews"
    params = {
        "limit": str(100),
        "pretty": "true",
        "client_id": client_id,
    }

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # get list of users
    with open(user_file, "r") as f:
        user_data = json.load(f)

    # get reviews and save
    for user in tqdm(user_data):
        data = []
        user_name = user["username"]
        params["username"] = user_name
        for rating in np.arange(0, 5.25, 0.25):
            params["rating"] = str(rating)
            for num in range(0, 1000, 100):
                params["skip"] = str(num)
                temp_data = query_atlas_api(url, params)
                review_data = temp_data["reviews"]
                if not review_data:
                    break
                for review in review_data:
                    review["user"]["username"] = user_name
                data.extend(review_data)

                if num == 900:
                    print(
                        f"{user_name} may have additional reviews beyond allowable query limit"
                    )

        if data:
            user_id = data[0]["user"]["id"]
            with open(os.path.join(output_dir, f"{user_id}.json"), "w") as f:
                json.dump(data, f)


def clean_reviews(input_dir, reviews_dir, users_dir):
    """
    Cleans the review data extracted from `compile_reviews()` by
    removing unnecessary data. Also extracts additional user data.
    Review and user data are saved to the specified dirs using the
    same filenames.

    Args:
        input_dir (str): output dir from `compile_reviews()`
        reviews_dir (str): new dir to save cleaned reviews data to
        users_dir (str): new dir to save additional user data extracted
            from review data

    Returns:
        None. Saves data to json files.
    """
    Path(reviews_dir).mkdir(parents=True, exist_ok=True)
    Path(users_dir).mkdir(parents=True, exist_ok=True)
    reviews_data_list = glob.glob(os.path.join(input_dir, "*.json"))
    for reviews in reviews_data_list:
        with open(reviews, "r") as f:
            reviews_data = json.load(f)

        date_check = pytz.utc.localize(datetime(1900, 1, 1))
        for review in reviews_data:
            user_data = review["user"]
            user_id = user_data["id"]

            review["user_id"] = user_id
            del review["user"]

            game_data = review["game"]
            game_id = game_data["id"]

            review["game_id"] = game_id
            del review["game"]

            if dateutil.parser.isoparse(review["date"]) > date_check:
                user_data_save = user_data

        new_reviews_path = os.path.join(reviews_dir, os.path.basename(reviews))
        with open(new_reviews_path, "w") as f:
            json.dump(reviews_data, f)

        new_user_path = os.path.join(users_dir, os.path.basename(reviews))
        with open(new_user_path, "w") as f:
            json.dump(user_data_save, f)


def compile_games(client_id, reviews_dir, output_dir):
    """
    Compiles game data queried from the Board Game Atlas
    API and saves data in json files each containing 100 games.
    Requires the review data from the Board Game Atlas API to
    be compiled using `compile_reviews()` to be able to query the
    games.

    Args:
        client_id (str): client id associated with the Board Game
            Atlas API
        reviews_dir (str): output dir from `compile_reviews()` or
            `clean_reviews()`
        output_file (str): output directory

    Returns:
        None. Saves data to json files.
    """

    # get list of games
    reviews_data_list = glob.glob(os.path.join(reviews_dir, "*.json"))
    game_set = set()
    for reviews in reviews_data_list:
        with open(reviews, "r") as f:
            reviews_data = json.load(f)

        for review in reviews_data:
            game_set.add(review["game"]["id"])

    chunked_game_ids = utils.create_chunks(list(game_set), 10)

    url = "https://api.boardgameatlas.com/api/search"
    params = {
        "pretty": "true",
        "client_id": client_id,
    }

    # extract game data and save
    game_counter = 0
    output_counter = 1
    data = []
    for idx, chunk in tqdm(enumerate(chunked_game_ids)):
        params["ids"] = ",".join(str(x) for x in chunk)
        temp_data = query_atlas_api(url, params)
        game_data = temp_data["games"]
        if not game_data:
            continue
        for game in game_data:
            now = datetime.now(timezone.utc)
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
            game["datetime_extracted"] = dt_string
        data.extend(game_data)

        game_counter += 10
        if game_counter % 100 == 0:
            with open(
                os.path.join(output_dir, f"{str(output_counter).zfill(8)}.json"),
                "w",
            ) as f:
                json.dump(data, f)
            data = []
            output_counter += 1


def compile_prices(client_id, reviews_dir, output_dir):
    """
    Compiles price data queried from the Board Game Atlas
    API and saves data in json files using the game id.
    Requires the review data from the Board Game Atlas API to
    be compiled using `compile_reviews()` to be able to query the
    games.

    Args:
        client_id (str): client id associated with the Board Game
            Atlas API
        reviews_dir (str): output dir from `compile_reviews()` or
            `clean_reviews()`
        output_file (str): output directory

    Returns:
        None. Saves data to json files.
    """
    reviews_data_list = glob.glob(os.path.join(reviews_dir, "*.json"))
    game_set = set()

    # get list of games to check the price
    for reviews in reviews_data_list:
        with open(reviews, "r") as f:
            reviews_data = json.load(f)

        for review in reviews_data:
            game_set.add(str(review["game"]["id"]))

    url = "https://api.boardgameatlas.com/api/game/prices"
    params = {
        "pretty": "true",
        "client_id": client_id,
    }

    # check already parsed games
    price_set = set()
    for item in Path(output_dir).iterdir():
        if item.is_dir():
            price_data_list = glob.glob(os.path.join(output_dir, item, "*.json"))
            for price_file in price_data_list:
                price_set.add(os.path.basename(price_file).split(".")[0])

    # removes already parsed games from the set
    game_set = game_set.difference(price_set)

    # extract and save price data
    for idx, game in tqdm(enumerate(list(game_set))):
        params["game_id"] = str(game)
        temp_data = query_atlas_api(url, params)
        price_data = temp_data["gameWithPrices"]
        if not price_data:
            continue

        for key, val in price_data.items():
            key_dir = os.path.join(output_dir, key)
            if not os.path.isdir(key_dir):
                os.makedirs(key_dir)
            if val:
                for game in val:
                    game["price_category"] = key
                with open(
                    os.path.join(key_dir, f"{game}.json"),
                    "w",
                ) as f:
                    json.dump(val, f)


def combine_files(input_dir, output_dir=None):
    """
    Combines json files in a directory using the the first
    character of the file name based the set of ascii_letters
    and digits.

    Args:
        input_dir (str): directory with json files to be
            combined
        output_file (str): output directory to save combined
            files, if None, saves the files to the input_dir
            (default=None)

    Returns:
        None. Saves data to json files.
    """
    ascii_list = list(string.ascii_letters + string.digits)

    if not output_dir:
        output_dir = input_dir

    for val in ascii_list:
        temp_files = glob.glob(os.path.join(input_dir, val + "*.json"))

        if not temp_files:
            continue

        data = []
        for file in temp_files:
            with open(file, "r") as f:
                temp_data = json.load(f)

            if not isinstance(temp_data, list):
                data.append(temp_data)
            else:
                data.extend(temp_data)

        outfile = os.path.join(
            output_dir, os.path.basename(input_dir) + "_" + val + ".json"
        )

        with open(outfile, "w") as f:
            json.dump(data, f)
