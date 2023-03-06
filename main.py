import argparse
import csv
import os
import re
import sys
from textwrap import dedent

import pytest


def init_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("-j", "--job", dest="job_folder", help="job folder to process")
    parser.add_argument("-v", "--verbose", action="store_true", help="verbose output")
    return parser


def get_valid_args() -> argparse.Namespace:
    parser: argparse.ArgumentParser = init_argparse()
    args: argparse.Namespace = parser.parse_args()
    if args.job_folder is None:
        print("Error: no job selected")
        sys.exit(2)
    return args


def main() -> None:
    args: argparse.Namespace = get_valid_args()
    verbose: bool = args.verbose
    job_folder: str = args.job_folder
    if verbose:
        print(f"{job_folder = }")

    if verbose:
        print("Reading files...")
    baselight_file_path: str = os.path.join(job_folder, "Baselight_export.txt")
    xytech_file_path: str = os.path.join(job_folder, "Xytech.txt")
    if verbose:
        print(f"{baselight_file_path = }")
        print(f"{xytech_file_path = }")
        print("Opening files...")
    with open(baselight_file_path, "r") as baselight_file:
        baselight_content: str = baselight_file.read()
    with open(xytech_file_path, "r") as xytech_file:
        xytech_content: str = xytech_file.read()
    producer, operator, job, notes, xytech_paths = load_xytech_data(xytech_content)
    if verbose:
        print(f"{producer = }")
        print(f"{operator = }")
        print(f"{job = }")
        print(f"{notes = }")
        print(f"{xytech_paths = }")
        print(f"{len(xytech_paths) = }")

    if verbose:
        print("Writing output...")
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
        for line in baselight_content.splitlines():
            if not line:
                continue
            raw_baselight_path, raw_frame_numbers = split_baselight_line(line)
            if verbose:
                print("-----")
                print(f"{raw_baselight_path = }")
                print(f"{raw_frame_numbers = }")
            baselight_path: str = raw_baselight_path.replace("\\", "/").strip()
            frame_ranges: list[str] = get_frame_ranges(clean_numbers(raw_frame_numbers))
            for xytech_path in xytech_paths:
                common_path: str = reversed_common_path([xytech_path, baselight_path])
                if common_path.count("/") > 1:
                    if verbose:
                        print(f"{common_path = }")
                    for frame_range in frame_ranges:
                        csv_writer.writerow([xytech_path, frame_range])
                    break


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
    """Splits a baselight export file's line into a path and raw frame numbers.

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
    path: str = " ".join(line_tokens[:i]).replace("\\", "/")
    frame_numbers: list[str] = line_tokens[i:]
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


if __name__ == "__main__":
    main()


"""Sample content of output.csv:
Joan Jett/John Doe/Dirtfixing/Please clean files noted per Colorist Tom Brady


"/hpsans13/production/starwars/reel1/partA/1920x1080"/32-34
"/hpsans13/production/starwars/reel1/partA/1920x1080"/67-69
"/hpsans13/production/starwars/reel1/partA/1920x1080"/122-123
"/hpsans13/production/starwars/reel1/partA/1920x1080"/155
"/hpsans13/production/starwars/reel1/partA/1920x1080"/1023
"/hpsans13/production/starwars/reel1/partA/1920x1080"/1111-1112
"/hpsans13/production/starwars/reel1/partA/1920x1080"/1160
"/hpsans13/production/starwars/reel1/partA/1920x1080"/1201-1205
"/hpsans13/production/starwars/reel1/partA/1920x1080"/1211-1214
"/hpsans12/production/starwars/reel1/VFX/Hydraulx"/1251-1253
"/hpsans12/production/starwars/reel1/VFX/Hydraulx"/1260
"/hpsans12/production/starwars/reel1/VFX/Hydraulx"/1270-1272
"/hpsans13/production/starwars/reel1/partA/1920x1080"/1302-1303
"/hpsans13/production/starwars/reel1/partA/1920x1080"/1310
"/hpsans13/production/starwars/reel1/partA/1920x1080"/1500
"/hpsans13/production/starwars/reel1/partA/1920x1080"/5000-5002
"/hpsans15/production/starwars/pickups/shot_1ab/1920x1080"/5010-5014
"/hpsans13/production/starwars/reel1/partA/1920x1080"/5111
"/hpsans13/production/starwars/reel1/partA/1920x1080"/5122
"/hpsans13/production/starwars/reel1/partA/1920x1080"/5133
"/hpsans13/production/starwars/reel1/partA/1920x1080"/5144
"/hpsans13/production/starwars/reel1/partA/1920x1080"/5155
"/hpsans13/production/starwars/reel1/partA/1920x1080"/5166
"/hpsans13/production/starwars/reel1/VFX/Framestore"/6188-6191
"/hpsans13/production/starwars/reel1/partA/1920x1080"/6200-6201
"/hpsans13/production/starwars/reel1/partA/1920x1080"/6209
"/hpsans13/production/starwars/reel1/partA/1920x1080"/6212
"/hpsans13/production/starwars/reel1/partA/1920x1080"/6219
"/hpsans13/production/starwars/reel1/partA/1920x1080"/6233-6234
"/hpsans13/production/starwars/reel1/partA/1920x1080"/6267
"/hpsans13/production/starwars/reel1/partA/1920x1080"/6269
"/hpsans13/production/starwars/reel1/partA/1920x1080"/6271
"/hpsans13/production/starwars/reel1/partA/1920x1080"/6278
"/hpsans13/production/starwars/reel1/partA/1920x1080"/6282
"/hpsans13/production/starwars/reel1/partA/1920x1080"/6288-6290
"/hpsans13/production/starwars/reel1/partA/1920x1080"/6292-6294
"/hpsans13/production/starwars/reel1/partB/1920x1080"/6409-6411
"/hpsans13/production/starwars/reel1/partB/1920x1080"/6413
"/hpsans13/production/starwars/reel1/partB/1920x1080"/6450
"/hpsans13/production/starwars/reel1/partB/1920x1080"/6666-6668
"/hpsans13/production/starwars/reel1/partB/1920x1080"/6670-6671
"/hpsans13/production/starwars/reel1/partB/1920x1080"/6680-6684
"/hpsans14/production/starwars/reel1/VFX/AnimalLogic"/6832-6834
"/hpsans14/production/starwars/reel1/VFX/AnimalLogic"/6911-6914
"/hpsans13/production/starwars/reel1/partB/1920x1080"/8845
"/hpsans15/production/starwars/pickups/shot_1ab/1920x1080"/10001-10002
"/hpsans15/production/starwars/pickups/shot_1ab/1920x1080"/10008
"/hpsans15/production/starwars/pickups/shot_1ab/1920x1080"/11113
"/hpsans13/production/starwars/reel1/partB/1920x1080"/12011
"/hpsans13/production/starwars/reel1/partB/1920x1080"/12021
"/hpsans13/production/starwars/reel1/partB/1920x1080"/12031
"/hpsans13/production/starwars/reel1/partB/1920x1080"/12041
"/hpsans13/production/starwars/reel1/partB/1920x1080"/12051
"/hpsans13/production/starwars/reel1/partB/1920x1080"/12111
"/hpsans13/production/starwars/reel1/partB/1920x1080"/12121
"/hpsans13/production/starwars/reel1/partB/1920x1080"/12131
"/hpsans13/production/starwars/reel1/partB/1920x1080"/12141
"""

###############################################################################

"""Sample verbose output to the console:
> py main.py -j files -v
job_folder = 'files'
Reading files...
baselight_file_path = 'files\\Baselight_export.txt'
xytech_file_path = 'files\\Xytech.txt'
Opening files...
producer = 'Joan Jett'
operator = 'John Doe'
job = 'Dirtfixing'
notes = 'Please clean files noted per Colorist Tom Brady '
xytech_paths = ['/hpsans13/production/starwars/reel1/partA/1920x1080', '/hpsans12/production/starwars/reel1/VFX/Hydraulx', '/hpsans13/production/starwars/reel1/VFX/Framestore', '/hpsans14/production/starwars/reel1/VFX/AnimalLogic', '/hpsans13/production/starwars/reel1/partB/1920x1080', '/hpsans15/production/starwars/pickups/shot_1ab/1920x1080']
len(xytech_paths) = 6
Writing output...
-----
raw_baselight_path = '/images1/starwars/reel1/partA/1920x1080'
raw_frame_numbers = ['32', '33', '34', '67', '68', '69', '122', '123', '155', '1023', '1111', '1112', '1160', '1201', '1202', '1203', '1204', '1205', '1211', '1212', '1213', '1214']
common_path = '/starwars/reel1/partA/1920x1080'
-----
raw_baselight_path = '/images1/starwars/reel1/VFX/Hydraulx'
raw_frame_numbers = ['1251', '1252', '1253', '1260', '<err>', '1270', '1271', '1272', '']
common_path = '/starwars/reel1/VFX/Hydraulx'
-----
raw_baselight_path = '/images1/starwars/reel1/partA/1920x1080'
raw_frame_numbers = ['1302', '1303', '1310', '1500', '5000', '5001', '5002']
common_path = '/starwars/reel1/partA/1920x1080'
-----
raw_baselight_path = '/images1/starwars/pickups/shot_1ab/1920x1080'
raw_frame_numbers = ['5010', '5011', '5012', '5013', '5014']
common_path = '/starwars/pickups/shot_1ab/1920x1080'
-----
raw_baselight_path = '/images1/starwars/reel1/partA/1920x1080'
raw_frame_numbers = ['5111', '5122', '5133', '5144', '5155', '5166']
common_path = '/starwars/reel1/partA/1920x1080'
-----
raw_baselight_path = '/images1/starwars/reel1/VFX/Framestore'
raw_frame_numbers = ['6188', '6189', '6190', '6191']
common_path = '/starwars/reel1/VFX/Framestore'
-----
raw_baselight_path = '/images1/starwars/reel1/partA/1920x1080'
raw_frame_numbers = ['6200', '6201', '6209', '6212', '6219', '6233', '6234', '6267', '6269', '6271', '6278', '6282', '6288', '6289', '6290', '6292', '6293', '6294']
common_path = '/starwars/reel1/partA/1920x1080'
-----
raw_baselight_path = '/images1/starwars/reel1/partB/1920x1080'
raw_frame_numbers = ['6409', '6410', '6411', '6413', '6450', '6666', '6667', '6668', '6670', '6671', '6680', '6681', '6682', '6683', '6684']
common_path = '/starwars/reel1/partB/1920x1080'
-----
raw_baselight_path = '/images1/starwars/reel1/VFX/AnimalLogic'
raw_frame_numbers = ['6832', '6833', '6834', '6911', '6912', '6913', '6914']
common_path = '/starwars/reel1/VFX/AnimalLogic'
-----
raw_baselight_path = '/images1/starwars/reel1/partB/1920x1080'
raw_frame_numbers = ['8845']
common_path = '/starwars/reel1/partB/1920x1080'
-----
raw_baselight_path = '/images1/starwars/pickups/shot_1ab/1920x1080'
raw_frame_numbers = ['10001', '10002', '10008', '11113', '']
common_path = '/starwars/pickups/shot_1ab/1920x1080'
-----
raw_baselight_path = '/images1/starwars/reel1/partB/1920x1080'
raw_frame_numbers = ['12011', '12021', '12031', '12041', '12051', '12111', '12121', '12131', '12141', '<null>']
common_path = '/starwars/reel1/partB/1920x1080'
"""  # noqa: E501


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
