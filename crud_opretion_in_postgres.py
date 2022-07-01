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
        insert_data = """insert into public.employee_data (e_id, e_name,salary,dob) values (2,'akash',30000,
        '1999-11-22')"""
        cursor.execute(insert_data)
        connection.commit()
        logging.info("employee data inserted")
        cursor.execute("select * from employee_data")
        logging.info(cursor.fetchall())

        # for update query
        update_query = "update employee_data set e_name='rajesh' where e_id=2;"
        cursor.execute(update_query)
        connection.commit()
        logging.info("e_name is updated")
        cursor.execute("select * from employee_data")
        logging.info(cursor.fetchall())

        # for deleted query
        deleted_query = "delete from employee_data  where e_id=2;"
        cursor.execute(deleted_query)
        connection.commit()
        logging.info("data is deleted")
        cursor.execute("select * from employee_data")
        logging.info(cursor.fetchall())

    except (Exception, psycopg2.Error) as Error:
        print("error while inserted")


if __name__ == "__main__":
    connect()
