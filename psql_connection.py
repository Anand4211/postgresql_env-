import logging

import psycopg2
from psycopg2.extras import LoggingConnection

from config import config

logging.basicConfig(filename='example.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(message)s ')
logger = logging.getLogger("logger information")
params = config()


def connect():
    try:

        connection = psycopg2.connect(connection_factory=LoggingConnection, **params)
        connection.initialize(logger)
        cursor = connection.cursor()
        return cursor, connection

    except (Exception, psycopg2.Error) as error:
        logger.error("Error in create operation %s", error)


# drop table query
DROP = '''drop table  if exists '''

create_table1 = """create table"""

# select_query_table_name
select_query1 = "select * from "
# INSERT_QUERY
insert_user1 = 'insert into   '

# Deletion_query
delete_query1 = '''DELETE FROM user1 WHERE AGE =%s'''

# Update query

update_query1 = '''update user1 set name = %s where age =%s  '''
