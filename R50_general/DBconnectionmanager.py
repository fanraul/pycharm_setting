from sqlalchemy import create_engine
# import socket
# remote database
# DB_connection_string = 'mssql+pyodbc://Richmind:121357468@Richmind_Remote'
# local database

# hostname = socket.gethostname()
DB_connection_string = 'mssql+pyodbc://Richmind:121357468@Richmind'

class Dbconnectionmanager:
    def __init__(self, echo=False):
        self.engine = create_engine(DB_connection_string,echo = echo)
        self.conn = self.engine.connect()

    def getengine(self):
        return self.engine

    def getconn(self):
        return self.conn

    def closeconn(self):
        self.conn.close()

if __name__ == '__main__':
    dbconmgr = Dbconnectionmanager(echo=True)
#get stock list
    import pandas as pd
    dfm_stockid = pd.read_sql_query('select * from stock_basic_info'
                             , dbconmgr.getengine())
    print(dfm_stockid)
    dbconmgr.closeconn()