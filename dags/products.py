import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from datetime import datetime
import credentials
from Library import to_dataframe


# Access to the database.
import psycopg2
# Credentials
connexion = psycopg2.connect(
    host=credentials.ip,
    port=credentials.port,
    database=credentials.database,
    user=credentials.user,
    password=credentials.password
)


def _new_platform_fee(fila_ext):

    cost = 0
    if pd.notnull(fila_ext[9]):
        cost = fila_ext[9]
    else:
        # Access to the database
        cursor = connexion.cursor()
        # Recover the necessary data from the products_silver table.
        sql = "select cost from platform_cost_silver " \
              "where platform = '{}' " \
              "and updated_at <= '{}' " \
              "order by updated_at desc " \
              "limit 1".format(fila_ext[2], fila_ext[5])
        cursor.execute(sql)
        if cursor.rowcount == 0:
            sql = "select cost from platform_cost_silver where platform = 'Everything else' " \
                  "and updated_at <= '{}' " \
                  "order by updated_at desc  " \
                  "limit 1".format(fila_ext[5])
            cursor.execute(sql)
        for fila_int in cursor:
            cost = fila_int[0]/100 * fila_ext[6]

    return cost


def _transport_cost(fila_ext):

    cost = 0
    # Access to the database
    cursor = connexion.cursor()
    # Recover the necessary data from the products_silver table.
    sql = "select transport_cost from transport_cost_silver " \
          "where country = '{}' " \
          "and updated_at <= '{}' " \
          "order by updated_at desc " \
          "limit 1".format(fila_ext[7], fila_ext[5])
    cursor.execute(sql)
    if cursor.rowcount == 0:
        sql = "select cost from platform_cost_silver where platform = 'OTHER' " \
              "and updated_at <= '{}' " \
              "order by updated_at desc  " \
              "limit 1".format(fila_ext[5])
        cursor.execute(sql)
    for fila_int in cursor:
        cost = fila_int[0]

    return cost


def _last_updated():

    cursor = connexion.cursor()
    sql = "select updated_at from products_gold order by updated_at desc limit 1"
    cursor.execute(sql)
    if cursor.rowcount == 0:
        return_dict = {'empty': True}
    else:
        df = to_dataframe(cursor, ['updated_at'])
        return_dict = {'empty': False,
                       'time': str(df.iloc[0]['updated_at'])}
    return return_dict


def _load_new_data_silver(ti):

    # Recover the message sent by the _last_updated function.
    data_dict = ti.xcom_pull(task_ids=['last_updated'])[0]
    # To access to the bronze table
    cursor_bronze = connexion.cursor()
    # To access to the silver table
    cursor_silver = connexion.cursor()

    # Build the query
    sql_bronze = " select b.license_plate, status, platform, b.created_at, shipped_at, b.updated_at, sold_price, " \
                 " country, channel_ref, platform_fee, grading_cat, grading_time " \
                 " from sold_products_bronze as b " \
                 " left join graded_products_bronze as g on b.license_plate = g.license_plate"
    if not data_dict['empty']:
        sql_bronze += " where b.updated_at > '{}'".format(data_dict['time'])
        sql_bronze += " and b.updated_at = b.created_at"

    # Recover the data
    cursor_bronze.execute(sql_bronze)

    # saved in the silver table
    sql_silver = "INSERT INTO products_silver (license_plate, status, platform, created_at, shipped_at, " \
                 "updated_at, sold_price, country, channel_ref, platform_fee, grading_cat, grading_time) " \
                 "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    for fila in cursor_bronze:
        new_fee = _new_platform_fee(fila)
        cursor_silver.execute(sql_silver,
                              (fila[0], fila[1], fila[2], fila[3], fila[4], fila[5], fila[6], fila[7], fila[8],
                               new_fee, fila[10].upper(), fila[11])
                              )
        connexion.commit()


def _load_new_data_gold(ti):

    # Recover the message sent by the _last_updated function.
    data_dict = ti.xcom_pull(task_ids=['last_updated'])[0]
    # To access to the gold table
    cursor_gold = connexion.cursor()
    # To access to the silver table
    cursor_silver = connexion.cursor()

    # Build the query
    sql_silver = " select b.license_plate, status, platform, b.created_at, shipped_at, b.updated_at, sold_price, " \
                 " country, channel_ref, platform_fee, b.grading_cat, g.cost, grading_time " \
                 " from products_silver as b " \
                 " left join grading_fees_silver as g on b.grading_cat = g.grading_cat"
    if not data_dict['empty']:
        sql_silver += " where b.updated_at > '{}'".format(data_dict['time'])
        sql_silver += " and b.updated_at = b.created_at"

    # Recover the data
    cursor_silver.execute(sql_silver)

    # saved in the gold table
    sql_gold = "INSERT INTO products_gold (license_plate, status, platform, created_at, shipped_at, " \
               "updated_at, sold_price, country, transport_cost, channel_ref, platform_fee, grading_cat, " \
               "grading_fee, grading_time)  values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    for fila in cursor_silver:
        transport_cost = _transport_cost(fila)
        cursor_gold.execute(sql_gold,
                            (fila[0], fila[1], fila[2], fila[3], fila[4], fila[5], fila[6], fila[7], transport_cost,
                             fila[8], fila[9], fila[10], fila[11], fila[12])
                            )
        connexion.commit()


def _load_updated_data_silver(ti):

    # Recover the message sent by the _last_updated function.
    data_dict = ti.xcom_pull(task_ids=['last_updated'])[0]
    # To access to the bronze table
    cursor_bronze = connexion.cursor()
    # To access to the silver table
    cursor_silver = connexion.cursor()

    # Build the query
    if not data_dict['empty']:
        sql_bronze = " select b.license_plate, status, platform, b.created_at, shipped_at, b.updated_at, sold_price, " \
                     " country, channel_ref, platform_fee, grading_cat, grading_time " \
                     " from sold_products_bronze as b " \
                     " left join graded_products_bronze as g on b.license_plate = g.license_plate"
        sql_bronze += " where b.updated_at > '{}'".format(data_dict['time'])
        sql_bronze += " and b.updated_at > b.created_at order by updated_at asc"

        # Recover the data
        cursor_bronze.execute(sql_bronze)

        # saved in the silver table
        sql_silver = "INSERT INTO products_silver (license_plate, status, platform, created_at, shipped_at, " \
                     "updated_at, sold_price, country, channel_ref, platform_fee, grading_cat, grading_time) " \
                     "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        for fila in cursor_bronze:
            sql_del = "DELETE FROM products_silver WHERE license_plate = '{}'".format(fila[0])
            cursor_silver.execute(sql_del)
            new_fee = _new_platform_fee(fila)
            cursor_silver.execute(sql_silver,
                                  (fila[0], fila[1], fila[2], fila[3], fila[4], fila[5], fila[6], fila[7], fila[8],
                                   new_fee, fila[10].upper(), fila[11])
                                  )
            connexion.commit()


def _load_updated_data_gold(ti):

    # Recover the message sent by the _last_updated function.
    data_dict = ti.xcom_pull(task_ids=['last_updated'])[0]
    # To access to the silver table
    cursor_silver = connexion.cursor()
    # To access to the gold table
    cursor_gold = connexion.cursor()

    # Build the query
    if not data_dict['empty']:
        sql_silver = " select b.license_plate, status, platform, b.created_at, shipped_at, b.updated_at, sold_price, " \
                     " country, channel_ref, platform_fee, b.grading_cat, g.cost, grading_time " \
                     " from products_silver as b " \
                     " left join grading_fees_silver as g on b.grading_cat = g.grading_cat"
        sql_silver += " where b.updated_at > '{}'".format(data_dict['time'])
        sql_silver += " and b.updated_at > b.created_at order by updated_at asc"

        # Recover the data
        cursor_silver.execute(sql_silver)

        # saved in the gold table
        sql_gold = "INSERT INTO products_gold (license_plate, status, platform, created_at, shipped_at, " \
                   "updated_at, sold_price, country, transport_cost, channel_ref, platform_fee, grading_cat, " \
                   "grading_fee, grading_time)  values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        for fila in cursor_silver:
            sql_del = "DELETE FROM products_gold WHERE license_plate = '{}'".format(fila[0])
            cursor_gold.execute(sql_del)
            transport_cost = _transport_cost(fila)
            cursor_gold.execute(sql_gold,
                                (fila[0], fila[1], fila[2], fila[3], fila[4], fila[5], fila[6], fila[7],
                                 transport_cost, fila[8], fila[9], fila[10], fila[11], fila[12])
                                )
            connexion.commit()


def _end_layer():
    connexion.close()


with DAG("products",
         start_date=datetime(2023, 8, 1),
         schedule_interval='*/10 * * * *',
         catchup=False) as dag:
    
    bronze_layer = EmptyOperator(task_id="bronze_layer")
    
    last_updated = PythonOperator(
        task_id="last_updated",
        python_callable=_last_updated
    )

    silver_layer = EmptyOperator(task_id="silver_layer")
    
    load_new_data_silver = PythonOperator(
        task_id="load_new_data_silver",
        python_callable=_load_new_data_silver
    )

    load_updated_data_silver = PythonOperator(
        task_id="load_updated_data_silver",
        python_callable=_load_updated_data_silver
    )

    golden_layer = EmptyOperator(task_id="golden_layer")

    load_new_data_gold = PythonOperator(
        task_id="load_new_data_gold",
        python_callable=_load_new_data_gold
    )

    load_updated_data_gold = PythonOperator(
        task_id="load_updated_data_gold",
        python_callable=_load_updated_data_gold
    )

    end_layer = PythonOperator(
        task_id="end_layer",
        python_callable=_end_layer
    )

    bronze_layer >> last_updated >> silver_layer

    silver_layer >> [load_new_data_silver, load_updated_data_silver] >> golden_layer
    
    golden_layer >> [load_new_data_gold, load_updated_data_gold] >> end_layer
