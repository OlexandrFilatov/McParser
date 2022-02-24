import psycopg2
import requests
from bs4 import BeautifulSoup
import re
import settings


class McParser:
    mc_url = """https://www.mcdonalds.com/bin/getProductItemList?country=US
        %20%20%20%20%20%20%20%20&language=en&showLiveData=true&item="""
    main_url = 'https://www.mcdonalds.com/us/en-us/full-menu.html'

    def get_product_url(self, id_list):
        hook = '"'
        url = f'{self.mc_url}{id_list[0].strip(hook)}()'

        for id_number in id_list[1:]:
            product_id = id_number.strip('"')
            url += f'-{product_id}()'
        url += '&nutrient_req=N'

        return url

    def get_all_products_json_data(self):
        main_products_page = requests.get(self.main_url)
        soup = BeautifulSoup(main_products_page.text, 'html.parser')

        product_ids = re.findall(r'"\d{6}"', str(soup))
        json_url = self.get_product_url(product_ids)

        json_products_response = requests.get(json_url)
        json_data = json_products_response.json()

        item_nutrition_list = []
        for item_id in range(len(product_ids)):
            item_name = json_data['items']['item'][item_id]['item_name']

            try:
                nutrient_dict = json_data['items']['item'][item_id]['nutrient_facts']['nutrient']
            except KeyError:
                continue

            item_calories = nutrient_dict[0]['value']
            item_total_fat = nutrient_dict[8]['value']
            item_total_carbs = nutrient_dict[4]['value']
            item_total_protein = nutrient_dict[3]['value']

            item_data = (item_name, item_calories, item_total_fat, item_total_carbs, item_total_protein)
            if item_data not in item_nutrition_list:
                item_nutrition_list.append(item_data)

        return item_nutrition_list


class DbManager(McParser):

    def get_remote_db_credentials(self):
        for _ in range(10):
            try:
                connection = psycopg2.connect(**settings.REMOTE_POSTGRES_CREDS)
                if connection:
                    cursor = connection.cursor()
                    return connection, cursor
            except:
                continue

    # def connect_to_db(self, func):
    #     def wrapper(*args):
    #         try:
    #             connection, cursor = self.get_db_credentials()
    #             return func(cursor=cursor, connection=connection, *args)
    #         except Exception as e:
    #             print(f"Error during working in func - {func.__name__}")
    #             print(e)
    #             connection.rollback()
    #         finally:
    #             connection.close()
    #     return wrapper

    def create_product_table(self, cursor, connection):
        cursor.execute("""CREATE TABLE IF NOT EXISTS mc_data (
                    item_name CHAR(30),
                    item_calories INT,
                    item_total_fat INT, 
                    item_total_carbs INT, 
                    item_total_protein INT
                    """)
        connection.commit()

    def write_to_db(self, to_db_list, table_name, connection=None, cursor=None, id_tag=None, header=[], on_conflict=False):
        try:
            if on_conflict:
                update_string = ','.join(["{0} = excluded.{0}".format(e) for e in (header)])
                """
               :param to_db_list: list of lists
               :param table_name: str name
               :param id_tag: primary key
               :param update_string: list of columns
               :param on_conflict: False by default
               :return: None
               """
            signs = '(' + ('%s,' * len(to_db_list[0]))[:-1] + ')'
            try:
                args_str = b','.join(cursor.mogrify(signs, x) for x in to_db_list)
                args_str = args_str.decode()
                insert_statement = """INSERT INTO %s VALUES """ % table_name
                conflict_statement = """ ON CONFLICT DO NOTHING"""
                if on_conflict:
                    conflict_statement = """ ON CONFLICT ("{0}") DO UPDATE SET {1};""".format(id_tag, update_string)
                cursor.execute(insert_statement + args_str + conflict_statement)
                connection.commit()
            except Exception as e:
                print(e)
                return False
            return True

        finally:
            connection.close()

    def get_local_db_credentials(self):
        for _ in range(10):
            try:
                connection = psycopg2.connect(**settings.LOCAL_POSTRGES_CREDS)
                if connection:
                    cursor = connection.cursor()
                    cursor.execute("""CREATE database products""")
                    connection.commit()

                    cursor.execute("""CREATE TABLE IF NOT EXISTS mc_data (
                                        item_name CHAR(30),
                                        item_calories INT,
                                        item_total_fat INT, 
                                        item_total_carbs INT, 
                                        item_total_protein INT
                                        """)
                    connection.commit()

                    return connection, cursor
            except Exception as e:
                print(e)
                continue

    def run(self):
        connection, cursor = self.get_local_db_credentials()
        print(connection, cursor)
        connection.close()
        # if connection:
        #     try:
        #         self.get_local_db_credentials()
        #         item_nutrition_list = self.get_all_products_json_data()
        #         self.write_to_db(item_nutrition_list, 'mc_data')
        #     finally:
        #         connection.close()


if __name__ == '__main__':
    parser = DbManager()
    parser.run()



# class Worker(McParser, DbManager):
#     def __init___(self):
#         connection, cursor = self.get_local_db_credentials()
#         if connection:
#             try:
#                 self.create_product_table(cursor, connection)
#                 item_nutrition_list = self.get_all_products_json_data()
#                 self.write_to_db(item_nutrition_list, 'mc_data')
#             finally:
#                 connection.close()
#
#
# if __name__ == '__main__':
#     connection, cursor = McParser.get_local_db_credentials()
#     if connection:
#         try:
#             self.create_product_table(cursor, connection)
#             item_nutrition_list = self.get_all_products_json_data()
#             self.write_to_db(item_nutrition_list, 'mc_data')
#         finally:
#             connection.close()



