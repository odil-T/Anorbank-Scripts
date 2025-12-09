"""
Use this script to preprocess the exported JIRA CSV files from various departments into one CSV file.

How to use:
1. Place the script in the same directory as the `Jira Dashboards TTM` folder. Ensure that only one `Jira Dashboards TTM` folder exists to avoid combining their data.
2. Run the script.
3. Your outputs will appear in the folder specified by the variable `OUTPUT_DIR`.
"""

import datetime
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from pandas import DataFrame

CURR_DATETIME = datetime.datetime.now()
SCRIPT_DIR = Path(__file__).resolve().parent

OUTPUT_DIR = SCRIPT_DIR / "outputs"
LOGS_DIR = OUTPUT_DIR / "logs"

timestamp = CURR_DATETIME.strftime("%Y_%m_%d__%H_%M_%S")
OUTPUT_CSV_PATH = OUTPUT_DIR / f"jira_data_cleaned_{timestamp}.csv"
OUTPUT_LOGS_PATH = LOGS_DIR / f"logs_{timestamp}.txt"

# Specify which columns you wish to extract and save in the final output file
COLUMNS_TEMPLATE = ("Date/Period", "Lead Time", "Cycle Time", "Blocked Time", "Time to Market", "Analysis Time")
COLUMNS_TEMPLATE_LOWER = [x.lower() for x in COLUMNS_TEMPLATE]


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def rename_excel_files(dir_to_search: Path) -> None:
    """
    Searches for and renames all "AVG.xlsx" files to their parent directories' names.

    The `dir_to_search`'s all child files and directories are searched.
    If a file called "AVG.xlsx" is found, it is renamed to its parent directory's name.

    Args:
        dir_to_search: Path of directory to search.
    """
    for avg_file in dir_to_search.rglob("AVG*.xlsx"):
        new_name = avg_file.parent / f"{avg_file.parent.name}.xlsx"
        avg_file.rename(new_name)


def get_excel_filepaths(dir_to_search: Path) -> list[Path]:
    """
    Searches for and obtains the absolute filepaths of certain Excel files.

    The `dir_to_search`'s all child files and directories are searched.
    The Excel's filepath is saved if the directory name and the child Excel's stem match.

    Args:
        dir_to_search: Path of directory to search.

    Returns:
        List of matching absolute Excel filepaths.
    """
    excel_filepaths = [
        excel_filepath
        for excel_filepath in dir_to_search.rglob("*.xlsx")
        if excel_filepath.stem == excel_filepath.parent.name
    ]
    return excel_filepaths


def preprocess_columns(df: DataFrame, excel_filepath: Path) -> DataFrame:
    """
    Helper function to preprocess the input dataframes' columns.

    The following preprocessing steps are done in order:
    1. `⊞` characters are removed from column names
    2. column names that are not listed in `COLUMNS_TEMPLATE` are removed (case-insensitive)
    3. columns are renamed to have the same cases as `COLUMNS_TEMPLATE`
    4. columns are reordered according to `COLUMNS_TEMPLATE`; if a column is missing, they are filled with `NaN`

    Args:
        df: Dataframe to preprocess
        excel_filepath: Excel file which is being read as the dataframe.

    Returns:
        Output dataframe after preprocessing.
    """
    df.columns = df.columns.str.replace("⊞", "")
    mask = df.columns.str.lower().isin(COLUMNS_TEMPLATE_LOWER)
    df = df.loc[:, mask]
    for col in COLUMNS_TEMPLATE:
        if col.lower() not in df.columns.str.lower():
            logging.info(f"Колонка '{col}' не существует в файле '{excel_filepath.name}'\n")

    col_rename_mapper = {}
    for c1 in df.columns:
        if c1 in COLUMNS_TEMPLATE:
            continue
        for c2 in COLUMNS_TEMPLATE:
            if c1.lower() == c2.lower():
                col_rename_mapper[c1] = c2
                break
    df = df.rename(columns=col_rename_mapper)

    df = df.reindex(columns=COLUMNS_TEMPLATE)
    return df


def split_date_col(df: DataFrame) -> DataFrame:
    """
    Splits and replaces the `Date/Period` column of an input dataframe into three columns: `Period Start`, `Period End`, and `Week Number`.

    Uses RegEx to parse the string.

    Args:
        df: Dataframe to transform.

    Returns:
        Dataframe with the new columns.
    """
    regex_pattern = r"(\d{1,2}/\w{3}/\d{2})\s+-\s+(\d{1,2}/\w{3}/\d{2})\s+\([Ww][Ee]{2}[Kk]\s+#(\d+)\)"
    df_ = df["Date/Period"].str.extract(regex_pattern).rename(
        columns={0: "Period Start", 1: "Period End", 2: "Week Number"})
    df.drop("Date/Period", axis=1, inplace=True)
    df = pd.concat([df_, df], axis=1)
    return df


def convert_time_cols(df: DataFrame, columns_to_process: list) -> DataFrame:
    """
    Converts the specified list of columns from `XXd XXh XXm` format to total hours.

    Uses RegEx to parse the string values. The RegEx pattern can handle cases such as `XXd Xh`, `Xm`, `XdXh`. Values that do not match the RegEx pattern are treated as NaN.
    Minutes are converted and ceiled to the nearest hour.
    Assumes that the input dataframe already has the columns given in `columns_to_process`.
    For example, `10d 5h 3m` is converted to `246`, `1m` is converted to `1`, `-` is converted to NaN.

    Args:
        columns_to_process: Time columns that must be converted.
        df: Dataframe whose time columns must be converted.

    Returns:
        Dataframe with the new columns.
    """

    def process_nans(row: pd.Series) -> pd.Series:
        """
        Fills NaN values of rows that have at least one numeric value with 0.
        Necessary to calculate the total hours downstream.
        """
        if row.notna().any():
            return row.fillna(0)
        return row

    regex_pattern = r"(?:(?P<Days>\d+)d\s*)?(?:(?P<Hours>\d+)h\s*)?(?:(?P<Minutes>\d+)m\s*)?"

    for col in columns_to_process:
        if df[col].dtype != "object":
            continue
        df_dhm = df[col].str.extract(regex_pattern, expand=True)
        df_dhm = df_dhm.apply(pd.to_numeric)
        df_dhm = df_dhm.apply(process_nans, axis=1)
        total_hours = (df_dhm["Days"] * 24) + df_dhm["Hours"] + np.ceil(df_dhm["Minutes"] / 60)
        df[col] = total_hours
    return df


def main():
    ensure_dir(OUTPUT_DIR)
    ensure_dir(LOGS_DIR)

    logging.basicConfig(
        filename=OUTPUT_LOGS_PATH,
        level=logging.INFO,
    )

    rename_excel_files(SCRIPT_DIR)
    excel_filepaths = get_excel_filepaths(SCRIPT_DIR)

    transformed_dfs = []
    for filepath in excel_filepaths:
        df = pd.read_excel(filepath)
        df = preprocess_columns(df, filepath)
        time_columns = [col for col in COLUMNS_TEMPLATE if "time" in col.lower()]
        df = convert_time_cols(df, time_columns)
        df = split_date_col(df)
        df.insert(loc=0, column="Department Name", value=filepath.stem)
        transformed_dfs.append(df)

    final_df = pd.concat(transformed_dfs, ignore_index=True)
    final_df.to_csv(OUTPUT_CSV_PATH, index=False)


if __name__ == "__main__":
    main()
