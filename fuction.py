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

        create_t2 = """ drop table if exists car """
        cursor.execute(create_t2)
        connection.commit()

        create_table1 = """create table car(car_name varchar(30),car_price int)"""
        cursor.execute(create_table1)
        connection.commit()
        logging.info("employeew table created")

        insert_data = """insert into car( car_name, car_price) values ('audi',3333333),('scorpio',1600000 ),('creata',
        180000) """
        cursor.execute(insert_data)
        connection.commit()
        logging.info("car data inserted")

        function = """ Create function get_car_Price(Price_from int, Price_to int)
     returns int  
     language plpgsql  
     as  
    $$  
     Declare  
     car_count integer;  
    Begin  
       select count(*)   
       into car_count  
       from car  
       where car_price between Price_from and Price_to;  
       return Car_count;  
    End;  
    $$;  """
        # cursor.execute(function)

        # print("function is created")

        explain_quaries = """ Select get_car_Price(26000,1600000);  """
        cursor.execute(explain_quaries)
        logging.info(cursor.fetchall())

    except (Exception, psycopg2.Error) as error:
        print("Error in create operation", error)


if __name__ == "__main__":
    connect()