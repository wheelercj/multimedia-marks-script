Project 1: import/export script
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
Script is run daily, save company 3k-10k per use.

* Import file created from baselight (Baselight_export.txt)
* Import xytech work order (Xytech.txt)
* Script will parse data
* Computation done to match shareholder request, to replace file system from local baselight to facility
storage (remember color correcter's prefer local storage for bandwidth issues)
* Export CSV file ('/' indicates columns):
* Line 1: Producer / Operator / job /notes
* Line 4: show location / frames to fix
* Frames in consecutive order shown as ranges
