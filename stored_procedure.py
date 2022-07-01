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
        create_t2 = """ drop table if exists bank """
        cursor.execute(create_t2)
        connection.commit()

        create_table = (
            """create table bank (id int, name varchar(12),balance numeric(7,2) )"""
        )
        cursor.execute(create_table)
        connection.commit()
        logging.info("bank table created")

        insert_data = """insert into bank(id,name,balance) values(1,'adnan',40000.9),(2,'arman',8787.8) """
        cursor.execute(insert_data)
        connection.commit()
        logging.info("bank data inserted")

        procedure = """create procedure money_transfer4(sender int ,receiver int ,amount numeric)
           as 
        $$  
           begin

               update bank set balance = balance-amount
               where id =sender;

               update bank set balance = balance+amount
               where id =receiver;


           end;

        $$ language plpgsql"""

        # cursor.execute(procedure)
        # connection.commit()

        cursor.execute("call money_transfer4(1,2,3000)")

        connection.commit()

        cursor.execute("select * from bank")
        logging.info(cursor.fetchall())

        cursor.execute("call money_transfer4(1,2,3000)")

        connection.commit()

        cursor.execute("select * from bank")
        logging.info(cursor.fetchall())

    except (Exception, psycopg2.Error) as error:
        print("Error in create operation", error)


if __name__ == "__main__":
    connect()
