"""
Use this script to parse logcat from Android Studio when testing for Firebase events from ANORBANK app.

This script will parse the following fields and save them in an Excel file:
- new_event
- screen_from
- current_screen
- entry_point
- user_id
- device_id

Old events will be printed separately at the end. They are defined as events that are logged by ANORBANK app, yet are not present in CONFLUENCE docs.

How to use:
1. Test a certain flow (eg. My home new) using the ANORBANK app and collect the logs using Android Studio.
2. Paste the logs in `logcat_logs.txt`.
3. Prepare a list of events by running `docx_parser.py`. They should be saved in docx_parser_output.xlsx. This file
is used by this script as reference to new_events.
4. Run this file.
"""

import re
import pandas as pd

LOGS_FILE_PATH = "logcat_logs.txt"
OUTPUT_EXCEL_PATH = "flow_report.xlsx"

field_names = ['name', 'screen_from', 'current_screen', 'entry_point', 'user_id', 'device_id']

new_events = list(pd.read_excel("docx_parser_output.xlsx")["Results"])
old_events = []

final_dict = {"new_event": new_events}
for field in field_names[1:]:
    final_dict[field] = [None for x in new_events]

pattern = re.compile(r'(name|user_id|device_id|entry_point|current_screen|screen_from)=([^,}]+)')


with open(LOGS_FILE_PATH, "r") as f:
    for line in f.readlines():
        if "origin=auto" in line:  # skip system logs
            continue

        matches = pattern.findall(line)

        event_name = matches[0][1]
        if event_name in new_events:
            insert_idx = new_events.index(event_name)
            for match in matches[1:]:
                field_name, field_value = match
                final_dict[field_name][insert_idx] = field_value
        else:
            old_events.append(event_name)

final_df = pd.DataFrame(final_dict)
final_df.to_excel(OUTPUT_EXCEL_PATH, index=False)

for old_event in old_events:
    print(old_event)