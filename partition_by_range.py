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
        drop_table = '''drop table if exists countries1 cascade'''
        cursor.execute(drop_table)

        cursor = connection.cursor()

        create_table1 = """CREATE TABLE IF NOT EXISTS public.countries1
    (
    country_id integer NOT NULL,
    country_name character varying(100) ,
    iso_codes character varying(2) ,
    iso_codes_3 character varying(3) ,
    region character varying(100) ,
    sub_region character varying(100) ,
    CONSTRAINT countries1_pkey PRIMARY KEY (country_id)
   ) PARTITION BY RANGE (country_id); """
        cursor.execute(create_table1)
        connection.commit()
        logging.info("countries1 table created")

        create_partition = """CREATE TABLE public.countries_partition PARTITION OF public.countries1
                               FOR VALUES FROM (201) TO (300);"""
        cursor.execute(create_partition)
        connection.commit()
        logging.info("create_partition table created")

        create_partition1 = """CREATE TABLE public.countries_partition1 PARTITION OF public.countries1
                                       FOR VALUES FROM (1) TO (3) partition by hash(country_id);"""
        cursor.execute(create_partition1)
        connection.commit()
        logging.info("create_partition1 table created")
        create_partition2 = """CREATE TABLE public.countries_partition9 PARTITION OF public.countries_partition1
                                             FOR VALUES with (modulus 3,remainder 1);"""
        cursor.execute(create_partition2)
        connection.commit()
        logging.info("create_partition2 table created")

        create_partition2 = """CREATE TABLE public.countries_partition2 PARTITION OF public.countries_partition1
                                       FOR VALUES with (modulus 3,remainder 2);"""
        cursor.execute(create_partition2)
        connection.commit()
        logging.info("create_partition2 table created")

        create_partition4 = """CREATE TABLE public.countries_partition4 PARTITION OF public.countries_partition1
                                                       FOR VALUES with (modulus 3,remainder 0);"""
        cursor.execute(create_partition4)
        connection.commit()
        logging.info("create_partition4 table created")

        create_partition5 = """CREATE TABLE public.countries_partition_4_200 PARTITION OF public.countries1
                                       FOR VALUES FROM (3) TO (200);"""
        cursor.execute(create_partition5)
        connection.commit()
        logging.info("create_partition5 table created")

        create_partition6 = """CREATE TABLE public.countries_partition6 PARTITION OF public.countries1
                               DEFAULT;"""
        cursor.execute(create_partition6)
        connection.commit()
        logging.info("create_partition6 table created")

        insert_data = """INSERT INTO countries1 (country_id, country_name, iso_codes,iso_codes_3,region,sub_region )
                          SELECT country_id, country_name, iso_code, iso_code_3,region,sub_region
                         FROM countrie ;  """
        cursor.execute(insert_data)
        connection.commit()
        logging.info("insertion occur data inserted")

        cursor.execute(
            """select * from  only countries1 """
        )
        logging.info(cursor.fetchall())

        cursor.execute(
            """select * from only countries_partition2 """
        )
        logging.info(cursor.fetchall())
        logging.info('check')
        cursor.execute(
            """select * from only countries_partition9 """
        )
        logging.info(cursor.fetchall())

        cursor.execute(
            """select * from only countries_partition4   """
        )
        logging.info(cursor.fetchall())

    except (Exception, psycopg2.Error) as error:
        print("Error in create operation", error)


if __name__ == "__main__":
    connect()
