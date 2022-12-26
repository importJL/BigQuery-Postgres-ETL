import pandas as pd
import os
from typing import List, Union
import pgsession.constants as c

def get_date_specs(df: pd.DataFrame) -> pd.DataFrame:
    '''
        TRANSFORMATION process involving processing date-time stamp into date format, quarter, year, month, and day of post date.

    Args:
        df: stored extracted data as dataframe.

    Returns:
        tmp: processed data as dataframe.
    '''
    tmp = df.copy()
    tmp['date'] = pd.to_datetime(tmp['time_ts'])
    tmp['quarter'] = tmp['date'].dt.quarter
    tmp['year'] = tmp['date'].dt.year
    tmp['month'] = tmp['date'].dt.month
    tmp['day'] = tmp['date'].dt.day
    return tmp
    
def get_word_text_len(df: pd.DataFrame) -> pd.DataFrame:
    '''
        TRANSFORMATION process involving calculating length of text (excluding any spaces of words).

    Args:
        df: stored extracted data as dataframe.

    Returns:
        tmp: processed data as dataframe.
    '''
    tmp = df.copy()
    tmp['text'] = tmp['text'].str.replace('\s+', '', regex=True)
    tmp['text_len'] = tmp['text'].apply(len)
    return tmp

def replace_na(df: pd.DataFrame, target_cols: Union[List[str], str]) -> pd.DataFrame:
    '''
        TRANSFORMATION process involving replacing NaN of target columns into 0.

    Args:
        df: stored extracted data as dataframe.
        target_cols (list[str], str): desired column(s) to have NaNs replaced.

    Returns:
        tmp: processed data as dataframe.
    '''
    cols = target_cols
    if type(target_cols) is not list:
        cols = [target_cols]
        
    tmp = df.copy()
    tmp[cols] = tmp[cols].fillna(0).astype(int)
    return tmp

    
if __name__ == "__main__":
    data_dir = os.path.join(os.getcwd(), c.DATA_FOLDER)
    
    # carry out data transformations
    df = pd.read_csv(os.path.join(data_dir, 'extracted_data.csv'))
    df2 = get_date_specs(df)
    df2 = get_word_text_len(df2)
    df2 = replace_na(df2, 'deleted')
    
    # make folder for storing processed data
    proc_dir = os.path.join(os.getcwd(), c.PROCESSED_FOLDER)
    if not os.path.isdir(proc_dir):
        os.mkdir(proc_dir)
    
    # store processed data as flat file (csv)
    df2.to_csv(os.path.join(proc_dir, 'processed_data.csv'), index=False, header=True)