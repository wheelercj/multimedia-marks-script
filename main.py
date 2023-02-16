import argparse
import os
import sys


def init_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("-j", "--job", dest="job_folder", help="job folder to process")
    parser.add_argument("-t", "--timecode", dest="timecode", help="timecode to process")
    parser.add_argument("-v", "--verbose", action="store_true", help="verbose output")
    return parser


def get_valid_args() -> argparse.Namespace:
    parser: argparse.ArgumentParser = init_argparse()
    args: argparse.Namespace = parser.parse_args()
    if args.job_folder is None:
        print("Error: no job selected")
        sys.exit(2)
    if args.timecode is None:
        print("Error: no timecode selected")
        sys.exit(2)
    return args


def main():
    args: argparse.Namespace = get_valid_args()
    baselight_file_path: str = os.path.join(args.job_folder, "Baselight_export.txt")
    xytech_file_path: str = os.path.join(args.job_folder, "Xytech.txt")
    with open(baselight_file_path, "r") as baselight_file:
        baselight_content: str = baselight_file.read()
    with open(xytech_file_path, "r") as xytech_file:
        xytech_content: str = xytech_file.read()
    # with open()  # TODO: open CSV file

    for line in baselight_content.splitlines():
        path, *frames = line.split(" ")

    # TODO: Find frame scratches/dirt from scanning and correct them.

    # TODO: export to .csv file ('/' indicates columns)


if __name__ == "__main__":
    main()
