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


def split_flame_line(line: str) -> tuple[str, str, list[str]]:
    """Splits a flame export file's line.

    The line is split into a storage path, a location path, and raw frame numbers.
    Assumes the line is either empty or contains a storage path, a location path, and
    raw frame numbers. Assumes the storage path does not contain any spaces. There may
    be instances of ``<err>``, ``<null>``, and/or empty strings mixed into the returned
    frame numbers.
    """
    if not line:
        return "", "", []
    line_tokens = line.split(" ")
    i = len(line_tokens)
    for token in reversed(line_tokens):
        if not token.isdigit() and token not in ("<err>", "<null>", ""):
            break
        i -= 1
    frame_numbers: list[str] = line_tokens[i:]
    storage_path, location_path = line_tokens[:i]
    storage_path = storage_path.replace("\\", "/")
    location_path = location_path.replace("\\", "/")
    return storage_path, location_path, frame_numbers


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
        "/net/flame-archive",
        "Avatar/reel1/VFX/Hydraulx",
        ["1260", "1261", "1262", "1267"],
    )


def test_split_flame_line_two_frames() -> None:
    assert split_flame_line(
        "/net/flame-archive Avatar/pickups/shot_5ab/1920x1080 9090 9091"
    ) == (
        "/net/flame-archive",
        "Avatar/pickups/shot_5ab/1920x1080",
        ["9090", "9091"],
    )


def test_split_flame_line_one_frame() -> None:
    assert split_flame_line("/net/flame-archive Avatar/reel1/VFX/Framestore 6195") == (
        "/net/flame-archive",
        "Avatar/reel1/VFX/Framestore",
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
