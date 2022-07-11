# All the logical code will come in this file.
# For each operation you have to create an individual method.


from psql_connection import *
import psycopg2

cursor, connection = connect()


def drop_table(table_name1):
    try:
        drop = DROP + table_name1 + " " + 'cascade'

        cursor.execute(drop)
        connection.commit()

        connection.close()
        logging.info('deleted')
        return retry()
    except (Exception, psycopg2.Error) as error:
        logging.error("Error in dropped operation", error)


def create_table(table_name, col_name):
    try:
        create = create_table1 + ' ' + table_name + col_name
        cursor.execute(create)
        connection.commit()
        cursor.close()
        connection.close()
        logging.info('table created')
        return retry()
    except (Exception, psycopg2.Error) as error:
        logging.error("Error in insertion operation", error)


def insert_in_table(table_name, index, age, name):
    try:
        insert_query = insert_user1 + table_name + ' ' + 'values (%s, %s,%s)'
        cursor.execute(insert_query, (index, age, name))
        connection.commit()
        cursor.close()
        connection.close()
        logging.info('value inserted')
        return retry()
    except (Exception, psycopg2.Error) as error:
        logging.error("Error in insertion operation", error)


def update_query(name, age):
    try:
        update = update_query1
        cursor.execute(update, (name, age))
        connection.commit()
        cursor.close()
        connection.close()
        logging.info('value updated')
        return retry()
    except (Exception, psycopg2.Error) as error:
        logging.error("Error in insertion operation", error)


def select_query(table_name):
    try:
        query = select_query1 + table_name
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        connection.close()
        logger.info("This is the values from this table:- %s", results)
        return retry()
    except (Exception, psycopg2.Error) as error:
        logger.error(" error in view operation %s", error)


def delete_query(del_age):
    try:
        deleted = delete_query1
        cursor.execute(deleted, (del_age,))
        connection.commit()
        cursor.close()
        connection.close()
        logging.info('value deleted')
        return retry()
    except (Exception, psycopg2) as error:
        logging.error("Error in deletion operation %s", error)


def retry():
    option = int(input(''' What U Want To Do :--\n 1. retry again \n 2. exit \n Enter here :'''))
    if option == 1:
        choosing()
    elif option == 2:
        return 0


def choosing():
    option = int(input(''' What U Want To Do :--\n For Drop Table Enter : 1 \n For Create Table Enter : 2 
 For Insert In Table Enter : 3 \n For Update In Enter : 4 \n For Delete In table Enter :5 
 For View In Table Enter :6 \n For Doing Nothing Enter :0 \n Enter here : '''))
    if option == 1:
        table_name1 = input('Table name:')
        drop_table(table_name1)
    elif option == 2:

        table_name = input(' Table name:')
        column_count = int(input(' How Many Column Want To Add:'))
        x = '('
        for i in range(column_count):
            if i == 0:
                col_name = input(" Write Column Name :")
                col_data_type = input(" Write Column Type :")
                x += col_name + ' ' + col_data_type
            else:
                col_name = input(" Write Column Name :")
                col_data_type = input(" Write Column Type :")
                x += ',' + col_name + ' ' + col_data_type
        create_table(table_name, x + ')')
    elif option == 3:
        table_name = input('table name:')
        n = int(input('how many values want to insert:'))
        for i in range(n):
            insert_index = int(input("write integer value only for insert index :"))
            insert_age = int(input("write integer value only for insert age  :"))
            insert_name = input("write string only keep size is only 20 for insert name :")
            insert_in_table(table_name, insert_index, insert_age, insert_name)

    elif option == 4:
        update_ages = int(input("fill integer data for updated age  :"))
        update_names = input("fill string value name :")
        update_query(update_names, update_ages)
    elif option == 5:
        deleted_age = int(input("write integer which is present in table for deleted age :"))
        delete_query(deleted_age)
    elif option == 0:
        return 0
    else:
        table_name = input('table name:')
        logger.info(select_query(table_name))


choosing()
