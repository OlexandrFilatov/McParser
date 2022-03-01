import mysql.connector
from settings import LOCAL_MYSQL_CREDS
from mysql.connector.errors import IntegrityError

class MySqLManager:
    def mysql_get_local_db_credentials(self):
        for _ in range(10):
            try:
                connection = mysql.connector.connect(**LOCAL_MYSQL_CREDS)
                if connection:
                    cursor = connection.cursor()
                    return connection, cursor
            except Exception as e:
                print(e)
                continue

    def mysql_create_db(self, mysql_cursor):
        mysql_cursor.execute('CREATE DATABASE IF NOT EXISTS mc_data')

    def mysql_create_product_table(self, mysql_cursor):
        mysql_cursor.execute('USE mc_data')
        mysql_cursor.execute("""CREATE TABLE IF NOT EXISTS product_nutritions (
                            item_name VARCHAR(255) PRIMARY KEY,
                            item_calories INT(8),
                            item_total_fat INT(8), 
                            item_total_carbs INT(8), 
                            item_total_protein INT(8))""")

    def mysql_write_to_db(self, to_db_list, connection=None, cursor=None):
        query = """INSERT INTO product_nutritions ( item_name,
                                        item_calories,
                                        item_total_fat,
                                        item_total_carbs,
                                        item_total_protein) 
                                VALUES (%s, %s, %s, %s, %s)"""
        try:
            cursor.executemany(query, to_db_list)
        except IntegrityError:
            print('False to add duplicated data')
        connection.commit()

