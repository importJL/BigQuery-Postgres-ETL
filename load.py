import os
import pandas as pd
import sqlalchemy
from sqlalchemy.dialects.postgresql import insert
from pgsession.models import DataRows
from pgsession.utils import PGSession
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
    tmp['date'] = pd.to_datetime(tmp['date'])
    return tmp

def bulk_insert(data: pd.DataFrame, session: sqlalchemy.orm.scoping.scoped_session) -> None:
    '''
        LOAD process involving storage of processed data into database.

    Args:
        data: processed data (as a dataframe)

    Returns:
        None
    '''
    values = [dict(row) for _, row in data.iterrows()]
    query_statement = insert(DataRows).values(values)
    try:
        query_statement = query_statement.on_conflict_do_nothing(
            constraint='loaded_tbl_pk'
        )
        session.execute(query_statement)
        session.commit()
    except Exception as e:
        session.rollback()
        print(f'Error: {e}')


if __name__ == '__main__':
    proc_dir = os.path.join(os.getcwd(), c.PROCESSED_FOLDER)
    df = pd.read_csv(os.path.join(proc_dir, 'processed_data.csv'))
    df2 = get_date_specs(df)
    
    pgsess = PGSession()

    check_tbl_query = "SELECT table_name FROM information_schema.tables WHERE table_name = '{}';".format(c.TABLE_NAME)
    tbl_list = pgsess.execute_query(check_tbl_query, return_result=True)

    if len(tbl_list) == 0:
        tbl_query = pd.io.sql.get_schema(df2, c.TABLE_NAME, keys='id', con=pgsess.engine)
        pgsess.execute_query(tbl_query)
    
    bulk_insert(df2, pgsess.session)
    
    pgsess.session.close()
    pgsess.engine.dispose()