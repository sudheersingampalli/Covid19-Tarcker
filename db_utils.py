import MySQLdb
from MySQLdb import cursors
import settings
import sqlalchemy
from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

class SQLAlchemyCFFDBConnection(object):
    """SQLAlchemy database connection"""

    def __init__(self):
        self.session = None

    def __enter__(self):
        self.engine = get_mysql_conn(settings.cff_host, settings.cff_user, settings.cff_password, settings.cff_schema,
                                    settings.cff_port, settings.enable_mysql_log)
        self.conn = self.engine.connect()
        SessionFactory = sessionmaker(self.conn)
        self.session = SessionFactory()
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()
        self.conn.close()

def get_mysql_conn(db_host, db_user, db_pw, db_schema, db_port, enable_log=False):
    '''Returns a connection object for Mysql DB'''

    url = 'mysql+mysqldb://{}:{}@{}:{}/{}'
    url = url.format(db_user, db_pw, db_host, db_port, db_schema)
    # The return value of create_engine() is our connection object
    engine = sqlalchemy.create_engine(url,
                                      connect_args={'charset': 'utf8'},
                                      isolation_level='AUTOCOMMIT')
    #if enable_log:
    #    event.listen(engine, "before_cursor_execute", before_cursor_execute)
    #    event.listen(engine, "after_cursor_execute", after_cursor_execute)

    return engine