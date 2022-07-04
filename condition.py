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
        print('Connecting to the PostgreSQL database...')
        connection = psycopg2.connect(connection_factory=LoggingConnection, **params)
        connection.initialize(logger)
        cursor = connection.cursor()
        drop_table = '''drop table if exists people cascade'''
        cursor.execute(drop_table)

        cursor = connection.cursor()
        create_table1 = """create table people(people_name varchar(30),people_age int)"""
        cursor.execute(create_table1)
        connection.commit()
        logging.info("people table created")

        insert_data = """insert into people( people_name, people_age) values ('deepaks',39),('rohit',45),('rahul',44) """
        cursor.execute(insert_data)
        connection.commit()
        logging.info("car data inserted")

        cursor.execute(
            """select people_name ,people_age from people  where people_name like '%_s' """
        )
        logging.info(cursor.fetchall())

    except (Exception, psycopg2.Error) as error:
        print("Error in create operation", error)


if __name__ == "__main__":
    connect()
