from google.cloud import bigquery
from google.oauth2 import service_account
import os
import pgsession.constants as c

SERVICE_ACCOUNT_JSON = os.environ['GOOGLE_APPLICATION_CREDENTIALS']

def big_query_to_csv(out_dir: str) -> None:
    '''
    Imitates "EXTRACT" process via data query from BigQuery table.

    Args:
        out_dir: directory path where data will be stored locally on computer.

    Returns:
        None
    '''
    query = '''SELECT * FROM bigquery-public-data.hacker_news.stories LIMIT 100;'''
    df = client.query(query).to_dataframe() 
    df.to_csv(os.path.join(out_dir, 'extracted_data.csv'), index=False, header=True)
    print('Data saved into CSV file.')

if __name__ == '__main__':
    # initiate google authorization and invoke BigQuery client
    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON, scopes=["https://www.googleapis.com/auth/cloud-platform"])
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    
    # make folder for storing extracted data
    data_dir = os.path.join(os.getcwd(), c.DATA_FOLDER)
    if not os.path.isdir(data_dir):
        os.mkdir(data_dir)

    # extract data as flat file (csv)
    big_query_to_csv(data_dir)