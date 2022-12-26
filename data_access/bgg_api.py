from datetime import datetime, timezone
import requests
import time
from tqdm import tqdm
import xml.etree.ElementTree as ET

import pandas as pd

import utils


def query_bgg_api(ids, endpoint):
    """
    Queries the BoardGameGeek API and returns an
    xml response if available.

    Args:
        ids (list(int)): ids to be requested
        endpoint (str): api endpoint to query

    Returns:
        xml tree (if no data, returns None)
    """

    ids = ",".join(str(x) for x in ids)
    url = f"https://www.boardgamegeek.com/xmlapi/{endpoint}/{ids}?stats=1"

    back_off = [10, 50, 100, 200, 300]
    tree = None
    for sleep_time in back_off:
        print(f"sleeping: {sleep_time}")
        time.sleep(sleep_time)
        try:
            resp = requests.get(url)
            if resp.status_code == 200:
                tree = ET.fromstring(resp.content)
                break
            if resp.status_code == 202:
                resp = requests.get(url)
                tree = ET.fromstring(resp.content)
                break
        except requests.exceptions.ChunkedEncodingError as e:
            print(e)
            print(f"Invalid chunk encoding for {ids}")

        except Exception as e:
            print(e)
            print(f"Un-handled error for {ids}")

    return tree


def get_games(game_ids):
    """
    Retrieve games data from the the BoardGameGeek API
    and returns as a Pandas DataFrame.

    Args:
        game_ids (list(int)): ids to be requested

    Returns:
        Pandas DataFrame (if no data, returns None)
    """

    tree = query_bgg_api(game_ids, "boardgames")
    rows = []
    if not tree:
        print(f"200 code not achieved for {game_ids}")
        return rows

    for child in tree:
        if not child.attrib:
            continue

        # extract id
        bgg_id = child.attrib["objectid"]

        # extract name
        name = ""
        value = child.findall("name")
        for v in value:
            if "primary" in set(v.attrib.keys()):
                name = v.text
        if not name:
            name = [x.text for x in value][0]

        # extract various attributes
        maxplayers = child.find("maxplayers").text
        maxplaytime = child.find("maxplaytime").text
        age = child.find("age").text
        minplayers = child.find("minplayers").text
        minplaytime = child.find("minplaytime").text
        year_published = child.find("yearpublished").text

        # extract attributes with multiple values
        value = child.findall("boardgameartist")
        artist = [x.text for x in value]
        value = child.findall("boardgamecategory")
        category = [x.text for x in value]
        value = child.findall("boardgamecompilation")
        compilation = [x.text for x in value]
        value = child.findall("boardgamedesigner")
        designer = [x.text for x in value]
        value = child.findall("boardgamefamily")
        family = [x.text for x in value]
        value = child.findall("boardgamemechanic")
        mechanic = [x.text for x in value]
        value = child.findall("boardgamepublisher")
        publisher = [x.text for x in value]

        # extract stats
        stats = child.find("statistics")
        ratings = stats.find("ratings")
        user_ratings = ratings.find("usersrated").text
        average_rating = ratings.find("average").text

        # compile row of data
        row = [
            bgg_id,
            maxplayers,
            maxplaytime,
            age,
            minplayers,
            minplaytime,
            name,
            year_published,
            artist,
            category,
            compilation,
            designer,
            family,
            mechanic,
            publisher,
            user_ratings,
            average_rating,
        ]
        rows.append(row)

    if not rows:
        print(f"No valid IDs for {game_ids}")
        return rows

    column_names = [
        "bgg_id",
        "maxplayers",
        "maxplaytime",
        "age",
        "minplayers",
        "minplaytime",
        "name",
        "year_published",
        "artist",
        "category",
        "compilation",
        "designer",
        "family",
        "mechanic",
        "publisher",
        "users_rated",
        "average_rating",
    ]

    df = pd.DataFrame(rows, columns=column_names)

    list_columns = [
        "artist",
        "category",
        "compilation",
        "designer",
        "family",
        "mechanic",
        "publisher",
    ]

    for col in list_columns:
        df[col] = [",".join(map(str, x)) for x in df[col]]

    # add extracted data datetime
    now = datetime.now(timezone.utc)
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    df["datetime_extracted"] = dt_string

    print(df.head())
    print(df.shape)
    return df


def get_geeklist(geeklist_id):
    """
    Retrieve geeklist data from the the BoardGameGeek API
    and returns as a Pandas DataFrame.

    Args:
        geeklist_id (list(int)): id to be requested

    Returns:
        Pandas DataFrame (if no data, returns None)
    """

    tree = query_bgg_api(geeklist_id, "geeklist")
    rows = []
    if not tree or tree.tag != "geeklist":
        print(f"200 code not achieved for {geeklist_id}")
        return rows

    # extract id
    geeklist_id = tree.attrib["id"]

    for child in tree:
        if not child.attrib:
            continue

        # extract attributes
        objtype = child.attrib["subtype"]
        game_id = child.attrib["objectid"]
        name = child.attrib["objectname"]
        user = child.attrib["username"]
        postdate = child.attrib["postdate"]
        bodytext = child.find("body").text

        # compile row of data
        row = [geeklist_id, objtype, game_id, name, user, postdate, bodytext]
        rows.append(row)

    if not rows:
        print(f"No valid data for {geeklist_id}")
        return rows

    column_names = [
        "geeklist_id",
        "objtype",
        "game_id",
        "name",
        "user",
        "postdate",
        "bodytext",
    ]

    df = pd.DataFrame(rows, columns=column_names)

    print(df.head())
    print(df.shape)
    return df


def compile_data(ids_list, file_out, api_func, chunk=None):
    """
    Compiles a dataframe from the BoardGameGeek API based on a list of
    ids and saves the data as a csv file.

    Args:
        ids_list (list(int)): ids to be requested
        file_out (str): csv file location for saved data
        api_func: function that that accepts a list of ids
            and returns a pandas dataframe either: get_games,
            or get_geeklist
        chunk (int): whether to chunk the ids into group
            (default=None)

    Returns:
        None. Saves data to a csv file.
    """

    if chunk:
        ids_list = utils.create_chunks(ids_list, chunk)

    df_list = []
    for ids in tqdm(list(ids_list)):
        temp_data = api_func(ids)
        if len(temp_data) > 0:
            df_list.append(temp_data)

    print(f"Joining dataframes and saving to: {file_out}")
    df = pd.concat(df_list)
    print(df.head())
    print(df.shape)
    df.to_csv(file_out, index=False)