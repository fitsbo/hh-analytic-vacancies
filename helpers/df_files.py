import shutil
from pathlib import Path

import pandas as pd

from helpers.logger import MyLogger
from helpers.misc_helpers import get_now_filename

CONFIG = {
    "FILENAME_LOG": "dffiles",
    "CONSOLE_LOG": True,
    "CONSOLE_LEVEL_LOG": "info",
}


logger = MyLogger(
    name=CONFIG["FILENAME_LOG"],
    stream=CONFIG["CONSOLE_LOG"],
    stream_level=CONFIG["CONSOLE_LEVEL_LOG"],
)


NA_VALUES = ["<NA>", "N/A", "NA", "NULL", "NaN", "n/a", "nan", "null"]


def load_df(src_directory: str, file_name: str, **kwargs):
    df = pd.DataFrame()
    file_path = Path(src_directory).joinpath(file_name)
    result = {"status": "", "df": pd.DataFrame()}
    try:
        df = pd.read_csv(file_path, na_values=NA_VALUES, **kwargs)
        result["status"] = 0
        result["df"] = df
        log_message = f"OK | READ files with mask {file_name} in {src_directory} to DataFrame, total {len(df)} rows"
    except Exception as error:
        result["status"] = 1
        log_message = (
            f"ERROR | READ files with mask {file_name} in {src_directory} error {error}"
        )
    logger.info(log_message)
    return result


def write_df(
    datasource: pd.DataFrame, dst_directory: str, name: str, timestamp=None, **kwargs
):
    df = pd.DataFrame.from_records(datasource)
    if timestamp is None:
        timestamp = get_now_filename()
    out_dir = Path(dst_directory)
    if not out_dir.exists():
        out_dir.mkdir()
        log_message = f"OK | WRITE directory {out_dir} created"
        logger.info(log_message)
    filename = out_dir.joinpath(name)
    mode = "w"
    header = True
    if filename.exists():
        mode = "a"
        header = False
    # with open(filename, mode, encoding="utf-8", newline="") as f:
    df.to_csv(
        filename,
        mode=mode,
        header=header,
        encoding="utf-8",
        index=False,
        # na_rep="NaN",
        **kwargs,
    )
    log_message = f"OK | WRITE {name} to {filename}"
    logger.info(log_message)


def move_files_processed(src_directory: str, dst_directory: str, file_name: str):
    target_directory = Path(dst_directory)
    if not target_directory.exists():
        target_directory.mkdir(parents=True)
        log_message = f"OK | Directory {dst_directory} was created"
        logger.info(log_message)
    file_path = Path(src_directory).joinpath(file_name)
    try:
        destination = target_directory / file_path.name
        shutil.move(str(file_path), str(destination))
        log_message = f"OK | MOVE File {file_path.name} moved to {dst_directory}"
        logger.info(log_message)
    except Exception as e:
        log_message = (
            f"ERROR | MOVE_PROCESSED Error occurred while moving file {file_path}: {e}"
        )
        logger.error(log_message)


def write_list_of_df(dst_directory: str, df_list: dict):
    result = 0
    for k, v in df_list.items():
        try:
            write_df(
                datasource=v["df_list"], dst_directory=dst_directory, name=v["filename"]
            )
            v["df_list"][:] = []
        except Exception as e:
            log_message = f"ERROR | WRITE_LIST_DF Error occurred while writing df {k} to file {v['filename']}: {e}"
            logger.error(log_message)
            result = 1
    return result
