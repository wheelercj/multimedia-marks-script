"""This script solves all of your problems.

Requires ffmpeg to be installed.

ffmpeg guide:
https://ostechnix.com/20-ffmpeg-commands-beginners/

mongodb guide:
https://www.w3schools.com/python/python_mongodb_getstarted.asp

each document of the jobs collection:
{
    "script_user": str,
    "machine": str,
    "user_on_file": str,
    "file_date": datetime,
    "submitted_date": datetime,
}

each document of the frames collection:
{
    "user_on_file": str,
    "file_date": datetime,
    "location": str,
    "frame_range": str,
}
"""
import argparse
import csv
import os
import re
import subprocess
from datetime import datetime
from io import BytesIO
from io import FileIO
from textwrap import dedent
from typing import Any
from typing import Callable

import openpyxl
import pymongo
import pytest
from openpyxl.drawing.image import Image as openpyxlImage
from PIL import Image as PILImage
from pymongo.collection import Collection
from pymongo.database import Database
from tqdm import tqdm


def init_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-f",
        "--file",
        type=argparse.FileType("r", encoding="utf-8"),
        action="append",  # allow multiple files
        dest="files",
        help="Baselight or Flame export file. Can be used multiple times.",
    )
    parser.add_argument(
        "-x",
        "--xytech",
        type=argparse.FileType("r", encoding="utf-8"),
        help="Xytech file",
    )
    parser.add_argument(
        "-p",
        "--process",
        type=str,
        dest="process_file_path",
        help="path of the video file to process",
    )
    parser.add_argument(
        "-o", "--output", choices=["CSV", "DB", "XLS"], help="output destination"
    )
    parser.add_argument("--verbose", action="store_true", help="verbose console output")
    return parser


def get_valid_args() -> argparse.Namespace:
    parser: argparse.ArgumentParser = init_argparse()
    args, unknown = parser.parse_known_args()
    if unknown:
        parser.error(
            f"Unknown arguments: {unknown}"
            "\nIf entering multiple files, use -f for each file."
        )
    if args.output is None:
        parser.error("Output destination must be specified")
    if args.output == "XLS" and args.process_file_path is None:
        parser.error("Process file must be specified for XLS output")
    return args


def main() -> None:
    args: argparse.Namespace = get_valid_args()
    output_destination: str = args.output
    assert output_destination in ["CSV", "DB", "XLS"]
    verbose: bool = args.verbose
    work_file_paths: list[FileIO] = args.files  # Baselight and Flame files
    xytech_file: FileIO = args.xytech
    process_file_path: str | None = args.process_file_path
    if verbose:
        print(f"{process_file_path = }")

    if output_destination == "XLS":
        assert process_file_path is not None
        export_files_to_xls(process_file_path, verbose)
    else:
        producer, operator, job, notes, xytech_paths = load_xytech_data(
            str(xytech_file.read())
        )
        if verbose:
            print(f"{producer = }")
            print(f"{operator = }")
            print(f"{job = }")
            print(f"{notes = }")
            print(f"{xytech_paths = }")  # the paths in the xytech file
            print(f"{len(xytech_paths) = }")
        if output_destination == "CSV":
            export_files_to_csv(
                producer, operator, job, notes, work_file_paths, xytech_paths, verbose
            )
        elif output_destination == "DB":
            export_files_to_db(work_file_paths, xytech_paths, verbose)
        else:
            raise NotImplementedError("Programmer error: invalid output format.")


def get_file_date(file_name: str) -> str:
    """Returns the date from a file name.

    The last 8 digits of the file name before the file extension must be the date in the
    format YYYYMMDD.
    """
    return file_name.rsplit(".", 1)[0][-8:]


def export_files_to_csv(
    producer: str,
    operator: str,
    job: str,
    notes: str,
    work_files: list[FileIO],
    xytech_paths: list[str],
    verbose: bool,
) -> None:
    if verbose:
        print("Writing output to output.csv")
    with open("output.csv", "w") as csv_file:
        csv_writer = csv.writer(
            csv_file,
            delimiter="/",
            lineterminator="\n",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
        )
        csv_writer.writerow([producer, operator, job, notes])
        csv_writer.writerow([])
        csv_writer.writerow([])
        for work_file in work_files:
            machine, user_on_file, file_date, work_file_content = get_work_file_data(
                work_file, verbose
            )
            export_file_to_csv_or_db(
                machine,
                work_file_content,
                user_on_file,
                file_date,
                xytech_paths,
                verbose,
                insert_row_into_csv,
                csv_writer.writerow,
            )


def export_files_to_db(
    work_files: list[FileIO],
    xytech_paths: list[str],
    verbose: bool,
) -> None:
    if verbose:
        print("Writing output to database")
    script_user: str = os.getlogin()
    mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
    db: Database = mongo_client["mydatabase"]
    jobs_collection: Collection = db["jobs"]
    frames_collection: Collection = db["frames"]
    for work_file in work_files:
        machine, user_on_file, file_date, work_file_content = get_work_file_data(
            work_file, verbose
        )
        jobs_collection.insert_one(
            {
                "script_user": script_user,
                "machine": machine,
                "user_on_file": user_on_file,
                "file_date": file_date,
                "submitted_date": datetime.now(),
            }
        )
        export_file_to_csv_or_db(
            machine,
            work_file_content,
            user_on_file,
            file_date,
            xytech_paths,
            verbose,
            insert_row_into_db,
            frames_collection.insert_one,
        )


def export_files_to_xls(process_file_path: str, verbose: bool) -> None:
    if verbose:
        print("Writing output to output.xls")

    video_frame_count, fps = get_video_data(process_file_path)
    if verbose:
        print(f"{video_frame_count = }")
        print(f"{fps = }")

    mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
    db: Database = mongo_client["mydatabase"]
    frames_collection: Collection = db["frames"]

    documents = frames_collection.find()
    db_data: list[tuple[str, str]] = [
        (document["location"], document["frame_range"])
        for document in documents
        if "-" in document["frame_range"]  # ignore individual frames
        and int(document["frame_range"].split("-")[1]) <= video_frame_count
        # ignore frames out of the range of the video
    ]
    if verbose:
        print(f"{len(db_data) = }")
        # print(f"{db_data = }")

    video_end_timecode: str = frame_to_timecode(video_frame_count, fps)
    if verbose:
        print(f"{video_end_timecode = }")

    wb: openpyxl.Workbook = openpyxl.Workbook()
    ws = wb.active
    assert ws is not None

    ws.column_dimensions["A"].width = 80  # type: ignore
    ws.column_dimensions["B"].width = 15  # type: ignore
    ws.column_dimensions["C"].width = 25  # type: ignore
    ws.column_dimensions["D"].width = 15  # type: ignore
    i = 1
    for location, frame_range in tqdm(db_data):
        time_range: str = frame_range_to_time_range(frame_range, fps)
        middle_frame_number: int = get_middle_frame_number(frame_range)
        middle_frame: openpyxlImage = get_frame(process_file_path, middle_frame_number)
        if verbose:
            print(f"\n{location = }")
            print(f"{frame_range = }")
            print(f"{time_range = }")
            print(f"{middle_frame_number = }")
        ws.append([location, frame_range, time_range])  # type: ignore
        ws.add_image(middle_frame, f"D{ws.max_row}")  # type: ignore
        ws.row_dimensions[i].height = 60  # type: ignore
        i += 1
    wb.save("output.xls")


def get_middle_frame_number(frame_range: str) -> int:
    start_frame_number_s, end_frame_number_s = frame_range.split("-")
    start_frame_number = int(start_frame_number_s)
    end_frame_number = int(end_frame_number_s)
    return start_frame_number + (end_frame_number - start_frame_number) // 2


def get_frame(video_file_path: str, frame_number: int) -> openpyxlImage:
    command = [
        "ffmpeg",
        "-i",
        video_file_path,
        "-vf",
        f"select=gte(n\\,{frame_number})",
        "-vframes",
        "1",
        "-f",
        "image2pipe",
        "-c:v",
        "bmp",
        "-",
    ]
    result: subprocess.CompletedProcess = subprocess.run(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if result.returncode != 0:
        raise Exception(result.stderr)
    image = PILImage.open(BytesIO(result.stdout))
    image.thumbnail((96, 74))
    return openpyxlImage(image)


def get_work_file_data(
    work_file: FileIO, verbose: bool
) -> tuple[str, str, datetime, str]:
    work_file_name: str = str(work_file.name).replace("\\", "/").split("/")[-1]
    if verbose:
        print(f"\t{work_file_name}")
    machine, user_on_file, file_date_and_extension = work_file_name.split("_")
    file_date: datetime = datetime.strptime(
        file_date_and_extension.split(".")[0], "%Y%m%d"
    )
    if verbose:
        print(f"\t\t{machine = }")
        print(f"\t\t{user_on_file = }")
        print(f"\t\t{file_date = }")
    work_file_content: str = str(work_file.read())
    return machine, user_on_file, file_date, work_file_content


def export_file_to_csv_or_db(
    machine: str,
    work_file_content: str,
    user_on_file: str,
    file_date: datetime,
    xytech_paths: list[str],
    verbose: bool,
    insert_row_wrapper: Callable,
    insert_row: Callable,
) -> None:
    for line in work_file_content.splitlines():
        if not line:
            continue
        if machine == "Baselight":
            path, raw_frame_numbers = split_baselight_line(line)
        elif machine == "Flame":
            path, raw_frame_numbers = split_flame_line(line)
        else:
            raise ValueError(f"Unknown machine: {machine}")
        if verbose:
            print("-----")
            print(f"{path = }")
            print(f"{raw_frame_numbers = }")
        frame_ranges: list[str] = get_frame_ranges(clean_numbers(raw_frame_numbers))
        for xytech_path in xytech_paths:
            common_path: str = reversed_common_path([xytech_path, path])
            if common_path.count("/") > 1:
                if verbose:
                    print(f"{common_path = }")
                for frame_range in frame_ranges:
                    insert_row_wrapper(
                        insert_row,
                        user_on_file,
                        file_date,
                        xytech_path,
                        frame_range,
                    )
                break


def insert_row_into_db(
    insert_one: Callable,
    user_on_file: str,
    file_date: str,
    location: str,
    frame_range: str,
) -> None:
    insert_one(
        {
            "user_on_file": user_on_file,
            "file_date": file_date,
            "location": location,
            "frame_range": frame_range,
        }
    )


def insert_row_into_csv(
    writerow: Callable,
    user_on_file: str,
    file_date: str,
    location: str,
    frame_range: str,
) -> None:
    writerow([location, frame_range])


def load_xytech_data(file_content: str) -> tuple[str, str, str, str, list[str]]:
    """Returns the producer, operator, job, notes, and paths from an Xytech file."""
    producer: str = get_field("Producer", file_content)
    operator: str = get_field("Operator", file_content)
    job: str = get_field("Job", file_content)
    _, location_and_notes = file_content.split("\nLocation:\n")
    location, notes = location_and_notes.split("\nNotes:\n")
    notes = notes.strip()
    paths: list[str] = location.strip().splitlines()
    return producer, operator, job, notes, paths


def get_field(label: str, content: str) -> str:
    """Uses regex to get a field from a string.

    Raises ValueError if the field is not found.

    Examples
    --------
    "Producer", "Xytech Workorder 1107\n\nProducer: Joan Jett"
        -> "Joan Jett"
    """
    field: re.Match[str] | None = re.search(rf"{label}: ([^\n]+)", content)
    if field is None:
        raise ValueError(f"Error: no {label} found in the Xytech file")
    assert field is not None
    return field.group(1).strip()


def reversed_common_path(paths: list[str]) -> str:
    """Returns the longest common sub-path of each path, starting from their ends.

    Examples
    --------
    [
        "/images1/starwars/reel1/partA/1920x1080",
        "/hpsans13/production/starwars/reel1/partA/1920x1080",
    ] -> "/starwars/reel1/partA/1920x1080"

    [
        "/hpsans13/production/starwars/reel1/partA/1920x1080",
        "/hpsans13/production/starwars/reel1/VFX/Hydraulx",
    ] -> ""

    [
        "/images1/starwars/reel1/partA/1920x1080",
        "/images1/starwars/reel1/partB/1920x1080",
    ] -> "/1920x1080"
    """
    if not paths:
        return ""
    for i, path in enumerate(paths):
        paths[i] = path.replace("\\", "/")
    if len(paths) == 1:
        return paths[0]
    reversed_paths: list[str] = [path[::-1] for path in paths]
    r_common_path: str = os.path.commonpath(reversed_paths).replace("\\", "/")
    if re.match(r"/:\w", r_common_path[-3:]):  # if there's a drive letter
        return r_common_path[::-1]
    if r_common_path == "":
        return ""
    return f"/{r_common_path[::-1]}"


def split_baselight_line(line: str) -> tuple[str, list[str]]:
    """Splits a Baselight export file's line into a path and raw frame numbers.

    Assumes the line is either empty or contains a path and raw frame numbers. There may
    be instances of ``<err>``, ``<null>``, and/or empty strings mixed into the returned
    frame numbers.
    """
    # Regex is not good to use here becauses it's more difficult to read and maintain.
    # If only I had thought of how to do this without regex earlier.
    # ^((?:(?:\w:)?[/\\][^/\n]+?)+)((?: (?:\d+|<null>|<err>))+) *$
    # Capture group 1 is the path and capture group 2 is the frame numbers, <null>s, and
    # <err>s. This pattern requires the re.MULTILINE flag.
    if not line:
        return "", []
    line_tokens: list[str] = line.split(" ")
    i = len(line_tokens)
    for token in reversed(line_tokens):
        if not token.isdigit() and token not in ("<err>", "<null>", ""):
            break
        i -= 1
    frame_numbers: list[str] = line_tokens[i:]
    path: str = " ".join(line_tokens[:i]).replace("\\", "/").strip()
    return path, frame_numbers


def split_flame_line(line: str) -> tuple[str, list[str]]:
    """Splits a Flame export file's line into a path and raw frame numbers.

    Assumes the line is either empty or contains a storage path, a location path, and
    raw frame numbers. Assumes the storage path does not contain any spaces. There may
    be instances of ``<err>``, ``<null>``, and/or empty strings mixed into the returned
    frame numbers.
    """
    if not line:
        return "", []
    line_tokens = line.split(" ")
    i = len(line_tokens)
    for token in reversed(line_tokens):
        if not token.isdigit() and token not in ("<err>", "<null>", ""):
            break
        i -= 1
    frame_numbers: list[str] = line_tokens[i:]
    storage_path, location_path = line_tokens[:i]
    path = os.path.join(storage_path, location_path).replace("\\", "/").strip()
    return path, frame_numbers


def clean_numbers(raw_frame_numbers: list[str]) -> list[int]:
    """Removes any non-numeric strings and converts the rest to ints."""
    return [
        int(frame_number)
        for frame_number in raw_frame_numbers
        if frame_number.isdigit()
    ]


def get_frame_ranges(frame_numbers: list[int]) -> list[str]:
    """Converts a list of frame numbers into a list of frame number ranges.

    Examples
    --------
    [1, 2, 3, 5, 6, 7] -> ["1-3", "5-7"]
    [1, 2, 3, 4, 5, 6] -> ["1-6"]
    [38] -> ["38"]
    [1, 3] -> ["1", "3"]
    """
    if not frame_numbers:
        return []
    frame_ranges: list[str] = []
    start: int = frame_numbers[0]
    end: int = frame_numbers[0]
    for i in range(1, len(frame_numbers)):
        if frame_numbers[i] == end + 1:
            end = frame_numbers[i]
        else:
            if start == end:
                frame_ranges.append(str(start))
            else:
                frame_ranges.append(f"{start}-{end}")
            start = frame_numbers[i]
            end = frame_numbers[i]
    if start == end:
        frame_ranges.append(str(start))
    else:
        frame_ranges.append(f"{start}-{end}")
    return frame_ranges


def frame_range_to_time_range(frame_range: str, fps: int) -> str:
    start_frame, end_frame = frame_range.split("-")
    start_timecode: str = frame_to_timecode(int(start_frame), fps)
    end_timecode: str = frame_to_timecode(int(end_frame), fps)
    return f"{start_timecode} - {end_timecode}"


def frame_to_timecode(frame_number: int | str, fps: int) -> str:
    """Converts a frame number to a timecode string.

    Timecodes are in the format hh:mm:ss:ff.

    Examples
    --------
    35 -> "00:00:01:11"
    1569 -> "00:01:05:09"
    14000 -> "00:09:43:08"
    """
    frame_number = int(frame_number)
    if frame_number < 0:
        raise ValueError("negative frame")
    second: int = frame_number // fps
    frame_number %= fps
    minute: int = second // 60
    second %= 60
    hour: int = minute // 60
    minute %= 60
    if hour >= 24:
        raise NotImplementedError("frame over 24 hours")
    return f"{ε(hour)}:{ε(minute)}:{ε(second)}:{ε(frame_number)}"


def ε(n: int) -> str:
    """Converts an integer to a string and zfills to a width of two."""
    return str(n).zfill(2)


def get_video_data(video_path: str) -> tuple[int, int]:
    ffmpeg_command: str = f"ffmpeg -i {video_path} -map 0:v:0 -c copy -f null -"
    ffmpeg_output: subprocess.CompletedProcess = subprocess.run(
        ffmpeg_command,
        shell=True,
        check=True,
        capture_output=True,
        text=True,
    )
    # print(f"{ffmpeg_output = }")
    frame_matches: list[Any] = re.findall(r"frame=\s*(\d+)", ffmpeg_output.stderr)
    if not frame_matches:
        raise ValueError("Could not find frame count in ffmpeg output.")
    frame_count: int = int(frame_matches[-1])
    fps_match: re.Match[Any] | None = re.search(r", (\d+) fps,", ffmpeg_output.stderr)
    if not fps_match:
        raise ValueError("Could not find fps in ffmpeg output.")
    fps: int = int(fps_match.group(1))
    return frame_count, fps


if __name__ == "__main__":
    main()


def test_load_xytech_data() -> None:
    assert (
        load_xytech_data(
            dedent(
                """
                Xytech Workorder 1107

                Producer: Joan Jett
                Operator: John Doe
                Job: Dirtfixing


                Location:
                /hpsans13/production/starwars/reel1/partA/1920x1080
                /hpsans12/production/starwars/reel1/VFX/Hydraulx
                /hpsans13/production/starwars/reel1/VFX/Framestore
                /hpsans14/production/starwars/reel1/VFX/AnimalLogic
                /hpsans13/production/starwars/reel1/partB/1920x1080
                /hpsans15/production/starwars/pickups/shot_1ab/1920x1080


                Notes:
                Please clean files noted per Colorist Tom Brady
                """
            )
        )
        == (
            "Joan Jett",
            "John Doe",
            "Dirtfixing",
            "Please clean files noted per Colorist Tom Brady",
            [
                "/hpsans13/production/starwars/reel1/partA/1920x1080",
                "/hpsans12/production/starwars/reel1/VFX/Hydraulx",
                "/hpsans13/production/starwars/reel1/VFX/Framestore",
                "/hpsans14/production/starwars/reel1/VFX/AnimalLogic",
                "/hpsans13/production/starwars/reel1/partB/1920x1080",
                "/hpsans15/production/starwars/pickups/shot_1ab/1920x1080",
            ],
        )
    )


def test_get_field() -> None:
    assert (
        get_field("Producer", "Xytech Workorder 1107\n\nProducer: Joan Jett")
        == "Joan Jett"
    )


def test_get_field_no_match() -> None:
    with pytest.raises(ValueError):
        get_field("Operator", "Xytech Workorder 3784\n\nProducer: Joan Jett")


def test_reversed_common_path() -> None:
    assert (
        reversed_common_path(
            [
                "/images1/starwars/reel1/partA/1920x1080",
                "/hpsans13/production/starwars/reel1/partA/1920x1080",
            ]
        )
        == "/starwars/reel1/partA/1920x1080"
    )


def test_reversed_common_path_no_common_path() -> None:
    assert (
        reversed_common_path(
            [
                "/hpsans13/production/starwars/reel1/partA/1920x1080",
                "/hpsans13/production/starwars/reel1/VFX/Hydraulx",
            ]
        )
        == ""
    )


def test_reversed_common_path_partial_common_path() -> None:
    assert (
        reversed_common_path(
            [
                "/images1/starwars/reel1/partA/1920x1080",
                "/images1/starwars/reel1/partB/1920x1080",
            ]
        )
        == "/1920x1080"
    )


def test_split_baselight_line() -> None:
    assert split_baselight_line(
        "/images1/starwars/reel1/VFX/Hydraulx 1251 1252 1253 1260 <err> 1270 1271 1272 "
    ) == (
        "/images1/starwars/reel1/VFX/Hydraulx",
        ["1251", "1252", "1253", "1260", "<err>", "1270", "1271", "1272", ""],
    )


def test_split_baselight_line_no_frames() -> None:
    assert split_baselight_line("/images1/starwars/reel1/VFX/Hydraulx") == (
        "/images1/starwars/reel1/VFX/Hydraulx",
        [],
    )


def test_split_baselight_line_path_with_spaces() -> None:
    assert split_baselight_line(
        "/images1/starwars/reel1/VFX/spaces and 3874 numbers 6188 6189 6190 6191"
    ) == (
        "/images1/starwars/reel1/VFX/spaces and 3874 numbers",
        ["6188", "6189", "6190", "6191"],
    )


def test_split_baselight_line_with_windows_path() -> None:
    assert split_baselight_line(
        "C:\\images1\\starwars\\reel1\\VFX\\Hydraulx 1251 1252 1253 1260 <err> 1270 "
    ) == (
        "C:/images1/starwars/reel1/VFX/Hydraulx",
        ["1251", "1252", "1253", "1260", "<err>", "1270", ""],
    )


def test_split_flame_line() -> None:
    assert split_flame_line(
        "/net/flame-archive Avatar/reel1/VFX/Hydraulx 1260 1261 1262 1267"
    ) == (
        "/net/flame-archive/Avatar/reel1/VFX/Hydraulx",
        ["1260", "1261", "1262", "1267"],
    )


def test_split_flame_line_two_frames() -> None:
    assert split_flame_line(
        "/net/flame-archive Avatar/pickups/shot_5ab/1920x1080 9090 9091"
    ) == (
        "/net/flame-archive/Avatar/pickups/shot_5ab/1920x1080",
        ["9090", "9091"],
    )


def test_split_flame_line_one_frame() -> None:
    assert split_flame_line("/net/flame-archive Avatar/reel1/VFX/Framestore 6195") == (
        "/net/flame-archive/Avatar/reel1/VFX/Framestore",
        ["6195"],
    )


def test_clean_numbers() -> None:
    assert clean_numbers(
        ["1251", "1252", "<null>", "1253", "1260", "<err>", "1270", "1271", "1272", ""]
    ) == [1251, 1252, 1253, 1260, 1270, 1271, 1272]


def test_get_frame_ranges_continuous() -> None:
    assert get_frame_ranges([1, 2, 3, 4, 5, 6]) == ["1-6"]


def test_get_frame_ranges_discontinuous() -> None:
    assert get_frame_ranges([1, 2, 3, 5, 6, 7]) == ["1-3", "5-7"]


def test_get_frame_ranges_one_number() -> None:
    assert get_frame_ranges([38]) == ["38"]


def test_get_frame_ranges_two_numbers() -> None:
    assert get_frame_ranges([1, 3]) == ["1", "3"]


def test_get_file_date_baselight() -> None:
    assert get_file_date("Baselight_GLopez_20230325.txt") == "20230325"


def test_get_file_date_flame() -> None:
    assert get_file_date("Flame_DFlowers_20230323.txt") == "20230323"


def test_get_file_date_xytech() -> None:
    assert get_file_date("Xytech_20230323.txt") == "20230323"
