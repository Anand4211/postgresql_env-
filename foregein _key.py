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
        drop_table = '''drop table if exists product cascade'''
        cursor.execute(drop_table)

        drop_table1 = '''drop table if exists supplier cascade'''
        cursor.execute(drop_table1)

        create_table = '''create table supplier (supplier_id int primary key ,supplier_name varchar(19))'''
        cursor.execute(create_table)
        connection.commit()

        create_table1 = '''create table product (product_id int primary key ,product_name varchar(15),supplier_id int, 
        foreign key(supplier_id) references supplier (supplier_id) on delete set null)'''
        cursor.execute(create_table1)
        connection.commit()

        insertion = '''insert into supplier values( 1,'rakesh'),(4,'prakash'),(6,'raja'),(9,'colliery')'''
        cursor.execute(insertion)
        connection.commit()

        insertion_product = '''insert into product values (1,'water',1),(2,'oil',null),(3,'water',1),(4,'laptop',9)'''
        cursor.execute(insertion_product)
        connection.commit()

        cursor.execute("""select * from supplier""")
        logging.info(cursor.fetchall())

        cursor.execute("""select * from product""")
        logging.info(cursor.fetchall())

        cursor.execute('''delete from supplier where supplier_id = 9''')
        logging.info('deleeted')

        cursor.execute('''select * from product''')
        logging.info(cursor.fetchall())

    except (Exception, psycopg2.Error) as error:
        print(error)


if __name__ == "__main__":
    connect()
