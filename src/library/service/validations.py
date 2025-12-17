import pandas as pd
import logging


def handle_missing_values(df: pd.DataFrame, df_name: str):
    """
    Remove rows with any NaN values or empty string values.
    Logs the number of rows removed.
    """
    original_len = len(df)
    df = df.dropna()
    string_cols = df.select_dtypes(include=['object', 'string']).columns
    for col in string_cols:
        df = df[df[col].astype(str).str.strip().str.len() > 0]
    removed = original_len - len(df)
    if removed > 0:
        logging.info(f"Removed {removed} rows from '{df_name}' with missing/empty values")
    return df


def validate_columns(df: pd.DataFrame, required_cols: list, df_name: str) -> pd.DataFrame:
    if required_cols:
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"Missing property for [{df_name}]: {missing}")
    extra = [i for i in df.columns if i not in required_cols]
    if extra:
        logging.info(
            f"[{df_name}] has extra columns (will be removed): {extra}")
        df = df.drop(columns=extra)
    return df


def handle_duplicates(df: pd.DataFrame, df_name: str):
    duplicates_df = df[df.duplicated()]
    if duplicates_df.size > 0:
        logging.info(f"Found duplicate [{df_name}]: {len(duplicates_df)}")
        return df.drop_duplicates()
    else:
        logging.info(f"no duplicates in [{df_name}]")
    return df
