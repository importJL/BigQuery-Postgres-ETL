from pgsession.utils import PGSession
import pgsession.constants as c


if __name__ == '__main__':
    pgsess = PGSession()
    
    drop_tbl_query = "DROP TABLE IF EXISTS {};".format(c.TABLE_NAME)
    pgsess.execute_query(drop_tbl_query)
    
    pgsess.session.close()
    pgsess.engine.dispose()