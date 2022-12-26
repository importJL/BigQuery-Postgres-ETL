import os
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker


class PGSession:
    def __init__(self, filename='db_creds.json'):
        self.creds_filename = filename
        url = self.__set_url()
        self.engine = self.__get_engine(url)
        self.__get_session()
        
    def __set_url(self):
        conn_file_path = os.getcwd()
        with open(os.path.join(conn_file_path, self.creds_filename), 'r') as creds:
            _conn_creds = json.load(creds)

        if _conn_creds.get('PASSWORD') != "":
            _url = 'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}'
        else:
            _url = 'postgresql://{USER}@{HOST}:{PORT}/{DATABASE}'
        return _url.format(**_conn_creds)
    
    @staticmethod
    def __get_engine(_url):
        return create_engine(_url, client_encoding='utf8')

    def __get_session(self):
        self.session = scoped_session(sessionmaker())
        self.session.configure(bind=self.engine, autoflush=False, expire_on_commit=False)

    def execute_query(self, query: str, return_result: bool = False):
        '''
            Sends query to database.  Returns data based on query type.

        Args:
            engine: connection instance to backend database
            query: SQL query as text.
            return_result: (optional) flag to enable return of query result

        Returns:
            results of query (if enabled) as a list.
        '''
        with self.engine.connect() as connection:
            result = connection.execute(query)
        
        if return_result:
            return result.fetchall()
        
        print('Query executed')