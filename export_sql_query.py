"""
Use this script for queries >1M rows to export to chunked Excel files.

HOW TO USE:
1. Ensure that the connection to the PostgreSQL database is working. Change your password if necessary.
2. Prepare your query to export by creating a materialized view called `sandbox.export_o` along with an `rn` column to enumerate the rows starting from 1.
    - Paste this for unordered row numbers: `ROW_NUMBER() OVER () AS rn`
4. Specify `TASK_NUMBER` for the filename.
5. Run the file.
6. Get your exports from the `outputs` file.
"""

import os
import math
import pandas as pd
import sqlalchemy
import urllib.parse
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import text


# Adjust these parameters
TASK_NUMBER = "cncp"

CHUNK_SIZE = 1_000_000
MATVIEW_NAME = "sandbox.export_o"
SAVE_DIR = "outputs"


load_dotenv()
Path(SAVE_DIR).mkdir(parents=True, exist_ok=True)

password = os.getenv("PW")
encoded_password = urllib.parse.quote(password)

engine = sqlalchemy.create_engine(f"postgresql://odilbek.tohirov:{encoded_password}@{os.getenv("IP")}:{os.getenv("PORT")}/dwh_db")

with engine.connect() as conn:
    total_rows = conn.execute(text(f"SELECT COUNT(*) FROM {MATVIEW_NAME}")).scalar()

NUM_CHUNKS = math.ceil(total_rows / CHUNK_SIZE)

for i in range(NUM_CHUNKS):
    lower = i * CHUNK_SIZE + 1
    upper = (i + 1) * CHUNK_SIZE

    if NUM_CHUNKS == 1:
        filename = f"{SAVE_DIR}/{TASK_NUMBER}.csv"
    else:
        filename = f"{SAVE_DIR}/{TASK_NUMBER}_part{i + 1}.csv"

    query = f"SELECT * FROM {MATVIEW_NAME} WHERE rn BETWEEN {lower} AND {upper} ORDER BY rn"
    df = pd.read_sql(query, engine)
    df.to_csv(filename, index=False)
    print(f"Saved {filename}")