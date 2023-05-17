# instructions

## project 3

See the lesson 8 slides and, if needed, the Panopto lecture recordings. Use the `twitch_nft_demo.mp4` file.

Solve:
1. Download my amazing VP video, https://mycsun.box.com/s/v55rwqlu5ufuc8l510r8nni0dzq5qki7
2. Run script with new argparse command `--process <video file>`
3. From (2) Call the populated database from proj2, find all ranges only that fall in the length of video from (1)
4. Using ffmpeg or 3rd party tool of your choice, to extract timecode from video and write your own timecode method to convert marks to timecode
5. New argparse output parameter for XLS with flag from (2) should export same CSV export, but in XLS with new column from files found from (3) and export their timecode ranges as well
6. Create Thumbnail (96x74) from each entry in (2), but middle most frame or closest to. Add to XLS file to it's corresponding range in new column

Optional (Extra Credit)
1. Render out each shot and upload them using API to frame.io or Shotgrid

Deliverables
1. Copy/Paste code
2. Excel file with new columns noted on Solve (5) and (6)

## project 2

See the lesson 6 slides and, if needed, the Panopto lecture recordings. Use the new files in the `import_files` folder.

## project 1: import/export script

* Arbitrary data file will be ingested
* Script will parse data
* Computation done to match shareholder request
* Export CSV file
* Needs to be crash proof (use argparse)

Find frame scratches/dirt from scanning and correct them.

The script is going to automate a bunch of tasks:

* Assist in Color Bay marking shots (4-8 hours, $1500 per hour for room, $100 hour operator).
* Verifying shots in file system (1-4 hours, $100 hour operator, $25 hour data op).
* Producer with a work order with correct files that need fixing (1 hour $50 hour producer).
* Edit/VFX receives a CSV with correct files (1 hour $90 hour specialist).

Script runs daily, save company 3k-10k per use.

* Import file created from baselight (Baselight_export.txt)
* Import xytech work order (Xytech.txt)
* Script will parse data
* Computation done to match shareholder request, to replace file system from local baselight to facility storage (remember color correcter's prefer local storage for bandwidth issues)
* Export CSV file ('/' indicates columns):
* Line 1: Producer / Operator / job /notes
* Line 4: show location / frames to fix
* Frames in consecutive order shown as ranges
