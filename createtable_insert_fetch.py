import logging
from psycopg2.extras import LoggingConnection
import psycopg2
from config import config

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("logger information")


def connect():
    try:

        params = config()

        # connect to the PostgreSQL server
        logging.info('Connecting to the PostgreSQL database...')
        connection = psycopg2.connect(connection_factory=LoggingConnection, **params)
        connection.initialize(logger)
        cursor = connection.cursor()

        create_t = """ drop table if exists employee_data """
        cursor.execute(create_t)
        connection.commit()

        create_table1 = """create table  employee_data (e_id int, e_name varchar(20)  ,salary int ,
        dob date); """
        cursor.execute(create_table1)
        connection.commit()
        logging.info("Table created")
        cursor.execute("""select e_id from employee_data;""")
        logging.info(cursor.fetchall())

    except (Exception, psycopg2.Error) as error:
        print("Error in create operation", error)


if __name__ == "__main__":
    connect()
