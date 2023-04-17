"""Answer these questions with database calls:

1. List all work done by user TDanza
2. All work done before 3-25-2023 on a Flame file
3. What work done on hpsans13 on date 3-26-2023
4. Name of all Autodesk Flame users
"""
from datetime import datetime

import pymongo
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from pymongo.database import Database


def main():
    mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
    db: Database = mongo_client["mydatabase"]
    frames_collection: Collection = db["frames"]

    work: list[tuple[str, list[str]]] = get_work_by_user("TDanza", frames_collection)
    print("Work done by TDanza:")
    for location, frame_range in work:
        print(f"\t{location} {frame_range}")

    work = get_work_before_date(
        datetime(year=2023, month=3, day=25),
        "Flame_DFlowers_20230323.txt",
        frames_collection,
    )
    print("Work done before 3-25-2023 on a Flame file (Flame_DFlowers_20230323.txt):")
    for location, frame_range in work:
        print(f"\t{location} {frame_range}")

    work = get_work_on_date_by_user(
        datetime(year=2023, month=3, day=26), "hpsans13", frames_collection
    )
    print("Work done on hpsans13 on date 3-26-2023:")
    for location, frame_range in work:
        print(f"\t{location} {frame_range}")

    flame_file_paths = [
        "Flame_DFlowers_20230323.txt",
        "Flame_DFlowers_20230326.txt",
        "Flame_DFlowers_20230327.txt",
        "Flame_MFelix_20230323.txt",
    ]
    names: list[str] = get_flame_users(flame_file_paths)
    print(f"Names of all Autodesk Flame users: {', '.join(names)}")


def get_work_by_user(
    user_name: str, frames_collection: Collection
) -> list[tuple[str, list[str]]]:
    """Returns the location and frame numbers of work done by a user."""
    cursor: Cursor = frames_collection.find({"user_on_file": user_name})
    locations: list[str] = []
    frame_ranges: list[list[str]] = []
    for item in cursor:
        locations.append(item["location"])
        frame_ranges.append(item["frame_range"])
    return list(zip(locations, frame_ranges))


def get_work_before_date(
    date: datetime, work_file_path: str, frames_collection: Collection
) -> list[tuple[str, list[str]]]:
    """Returns the location and frame numbers of work done before a date on a file."""
    work_file_path = work_file_path.replace("\\", "/")
    _, user_on_file, _ = work_file_path.split("/")[-1].split(".")[0].split("_")
    cursor: Cursor = frames_collection.find(
        {"user_on_file": user_on_file, "file_date": {"$lt": date}}
    )
    locations: list[str] = []
    frame_ranges: list[list[str]] = []
    for item in cursor:
        locations.append(item["location"])
        frame_ranges.append(item["frame_range"])
    return list(zip(locations, frame_ranges))


def get_work_on_date_by_user(
    date: datetime, user_name: str, frames_collection: Collection
) -> list[tuple[str, list[str]]]:
    """Returns the location and frame numbers of work done on a date by a user."""
    cursor: Cursor = frames_collection.find(
        {"user_on_file": user_name, "file_date": date}
    )
    locations: list[str] = []
    frame_ranges: list[list[str]] = []
    for item in cursor:
        locations.append(item["location"])
        frame_ranges.append(item["frame_range"])
    return list(zip(locations, frame_ranges))


def get_flame_users(flame_file_paths: list[str]) -> list[str]:
    """Returns the names of all Autodesk Flame users.

    Removes duplicates.
    """
    names: list[str] = []
    for path in flame_file_paths:
        name = path.split("_")[1]
        if name not in names:
            names.append(name)
    return names


if __name__ == "__main__":
    main()
