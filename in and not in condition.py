import psycopg2
import logging
from config import config

from psycopg2.extras import LoggingConnection

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("logger information")


def connect():
    """ Connect to the PostgreSQL database server """

    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        logging.info('Connecting to the PostgreSQL database...')
        connection = psycopg2.connect(connection_factory=LoggingConnection, **params)
        connection.initialize(logger)

        cursor = connection.cursor()

        cursor.execute('''select * from color where id between 2 and 5 order by color_name;''')
        logging.info(cursor.fetchall())

        cursor.execute('''select * from color where color_name in ('pink','sink') order by color_name;
                       ''')
        logging.info(cursor.fetchall())

        cursor.execute('''select * from color where color_name  not in ('pink','sink') order by color_name;
                           ''')
        logging.info(cursor.fetchall())

    except(Exception, psycopg2.Error) as e:
        print(e)


if __name__ == "__main__":
    connect()
