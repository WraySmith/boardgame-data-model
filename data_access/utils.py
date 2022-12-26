import glob
import json
import os


def create_chunks(id_list, n):
    """
    Breaks list into chunks of length n.

    args:
        id_list: list of ints
        n: int

    returns:
        generator of lists
    """

    for i in range(0, len(id_list), n):
        yield id_list[i : i + n]


def count_atlas_json(dir):
    """
    Counts total number of json items in a directory
    and returns count as a print statement
    """
    data_list = glob.glob(os.path.join(dir, "**/*.json"), recursive=True)

    counter = 0
    for data in data_list:
        with open(data, "r") as f:
            check_data = json.load(f)

        counter += len(check_data)

    print(f"Total count = {counter}")