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
        create_t2 = """ drop table if exists employeew """
        cursor.execute(create_t2)
        connection.commit()

        create_table1 = """create table employeew(id int ,employee_name varchar(30),employee_address varchar(20))"""
        cursor.execute(create_table1)
        connection.commit()
        logging.info("employeew table created")

        insert_data = """insert into employeew(id, employee_name, employee_address) values (2,'akash', 'noida,up'),(3,
        'uday','mumbai,maharastra'),(4,'adam','london') """
        cursor.execute(insert_data)
        connection.commit()
        logging.info("employee1 data inserted")

        explain_quaries = """ explain select * from employeew where id=2"""
        cursor.execute(explain_quaries)
        logging.info(cursor.fetchall())

        create_index = """create index idx_inde on employeew(employee_name)"""
        cursor.execute(create_index)

        connection.commit()
        logging.info("index  created on employee_name")

        explain_quaries = """ explain select * from employeew where id=2"""
        cursor.execute(explain_quaries)
        logging.info(cursor.fetchall())

        cursor.execute("""drop index idx_inde""")
        connection.commit()
        logging.info("index  deleted ")

    except (Exception, psycopg2.Error) as error:
        logging.info("Error in create operation", error)


if __name__ == "__main__":
    connect()
