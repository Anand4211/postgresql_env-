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
        drop_table = '''drop table if exists inherit cascade'''
        cursor.execute(drop_table)

        cursor = connection.cursor()
        create_table1 = """create table inherit(people_name varchar(30),people_age int)"""
        cursor.execute(create_table1)
        connection.commit()
        logging.info("inherit table created")

        create_table1 = """create table inherit_child() inherits (inherit) """
        cursor.execute(create_table1)
        connection.commit()
        logging.info("inherit child table created")

        insert_data = """insert into inherit( people_name, people_age) values ('deepaks',39),
        ('rohit',45),('rahul',44) """
        cursor.execute(insert_data)
        connection.commit()
        logging.info("inherit data inserted")

        insert_data = """insert into inherit_child( people_name, people_age) values ('deepiti',39),
               ('jazz',45),('salamander',41) """
        cursor.execute(insert_data)
        connection.commit()
        logging.info("inherit_child data inserted")

        cursor.execute(
            """select people_name ,people_age from  inherit  """
        )
        logging.info(cursor.fetchall())

        cursor.execute(
            """select people_name ,people_age from only  inherit  """
        )
        logging.info(cursor.fetchall())

        cursor.execute(
            """select people_name ,people_age from only  inherit_child  """
        )
        logging.info(cursor.fetchall())

        delete_table = "delete from inherit where people_age = 41"
        cursor.execute(delete_table)
        connection.commit()

        cursor.execute(
            """select people_name ,people_age from   inherit  """
        )
        logging.info(cursor.fetchall())

        cursor.execute(
            """select people_name ,people_age from only  inherit_child  """
        )
        logging.info(cursor.fetchall())

    except (Exception, psycopg2.Error) as error:
        print("Error in create operation", error)


if __name__ == "__main__":
    connect()
