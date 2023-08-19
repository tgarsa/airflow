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
    password=credentials.password)


def _last_updated_grading():
    '''
    :return: If the silver table is empty or when we saved the last updated.
    '''
    cursor = connexion.cursor()
    sql = "select updated_at from grading_fees_silver order by updated_at desc limit 1"
    cursor.execute(sql)
    if cursor.rowcount == 0:
        return_dict = {'empty': True}
    else:
        df = to_dataframe(cursor, ['updated_at'])
        return_dict = {'empty': False,
                       'time': str(df.iloc[0]['updated_at'])}
    return return_dict


def _last_updated_platform():

    cursor = connexion.cursor()
    sql = "select updated_at from platform_cost_silver order by updated_at desc limit 1"
    cursor.execute(sql)
    if cursor.rowcount == 0:
        return_dict = {'empty': True}
    else:
        df = to_dataframe(cursor, ['updated_at'])
        return_dict = {'empty': False,
                       'time': str(df.iloc[0]['updated_at'])}

    return return_dict


def _last_updated_transport():
    '''
    :return: If the silver table is empty or when we saved the last updated.
    '''
    cursor = connexion.cursor()
    sql = "select updated_at from transport_cost_silver order by updated_at desc limit 1"
    cursor.execute(sql)
    if cursor.rowcount == 0:
        return_dict = {'empty': True}
    else:
        df = to_dataframe(cursor, ['updated_at'])
        return_dict = {'empty': False,
                       'time': str(df.iloc[0]['updated_at'])}
    return return_dict


def _load_new_data_grading(ti):

    # Recover the message sent by the _last_updated function.
    data_dict = ti.xcom_pull(task_ids=['last_updated_grading'])[0]
    # To access to the bronze table
    cursor_bronze = connexion.cursor()
    # To access to the silver table
    cursor_silver = connexion.cursor()

    # Build the query
    # To  move the whole of the data, we don't have any data in the silver table.
    sql_bronze = "select grading_cat, cost, created_at, updated_at from grading_fees_bronze"
    if not data_dict['empty']:
        # To move only the new data.
        sql_bronze += " where updated_at > '{}'".format(data_dict['time'])
        sql_bronze += " and updated_at = created_at"

    # Recover the data
    cursor_bronze.execute(sql_bronze)

    # Move the data from the bronze to the silver table
    sql_silver = "INSERT INTO grading_fees_silver (grading_cat, cost, created_at, updated_at) values (%s,%s,%s,%s)"
    for fila in cursor_bronze:
        cursor_silver.execute(sql_silver, fila)
        connexion.commit()


def _load_new_data_platform(ti):

    # Recover the message sent by the _last_updated function.
    data_dict = ti.xcom_pull(task_ids=['last_updated_platform'])[0]
    # To access to the bronze table
    cursor_bronze = connexion.cursor()
    # To access to the silver table
    cursor_silver = connexion.cursor()

    # Build the query
    # if data_dict['empty']==True:
    sql_bronze = "select platform, cost, created_at, updated_at from platform_cost_bronze"
    if not data_dict['empty']:
        # sql_out = "select platform, cost, created_at, updated_at from platform_cost_bronze"
        sql_bronze += " where updated_at > '{}'".format(data_dict['time'])
        sql_bronze += " and updated_at = created_at"

    # Recover the data
    cursor_bronze.execute(sql_bronze)

    # saved in the silver table
    sql_silver = "INSERT INTO platform_cost_silver (platform, cost, created_at, updated_at) values (%s,%s,%s,%s)"
    for fila in cursor_bronze:
        cursor_silver.execute(sql_silver, fila)
        connexion.commit()

        
def _load_new_data_transport(ti):

    # Recover the message sent by the _last_updated function.
    data_dict = ti.xcom_pull(task_ids=['last_updated_transport'])[0]
    # To access to the bronze table
    cursor_bronze = connexion.cursor()
    # To access to the silver table
    cursor_silver = connexion.cursor()

    # Build the query
    # To  move the whole of the data, we don't have any data in the silver table.
    sql_bronze = "select country, transport_cost, created_at, updated_at from transport_cost_bronze"
    if not data_dict['empty']:
        # To move only the new data.
        sql_bronze += " where updated_at > '{}'".format(data_dict['time'])
        sql_bronze += " and updated_at = created_at"

    # Recover the data
    cursor_bronze.execute(sql_bronze)
    # TODO: Cosas a modificar y arreglar
    # Download the data more quickly.
    # cursor_bronze.fetchall()
    # cursor_bronze.fetchmany(2)
    # cursor_bronze.fetchone()

    # Move the data from the bronze to the silver table
    sql_silver = "INSERT INTO transport_cost_silver (country, transport_cost, created_at, updated_at) " \
                 "values (%s,%s,%s,%s)"
    for fila in cursor_bronze:
        cursor_silver.execute(sql_silver, fila)
        connexion.commit()
        # TODO: Cosas a modificar y arreglar
        # Close communication with the database
        # cursor_silver.close()
        # connexion.close()


def _load_updated_data_grading(ti):

    # Recover the message sent by the _last_updated function.
    data_dict = ti.xcom_pull(task_ids=['last_updated_grading'])[0]
    # To access to the bronze table
    cursor_bronze = connexion.cursor()
    # To access to the silver table
    cursor_silver = connexion.cursor()

    # We need to execute only if we have data in the silver table
    if not data_dict['empty']:
        # Look for the updated data in the bronze table
        sql_bronze = "select grading_cat, cost, created_at, updated_at from grading_fees_bronze"
        sql_bronze += " where updated_at > '{}'".format(data_dict['time'])
        sql_bronze += " and updated_at > created_at order by updated_at asc"

        # Recover the data
        cursor_bronze.execute(sql_bronze)

        # saved in the silver table
        sql_silver = "INSERT INTO grading_fees_silver (grading_cat, cost, created_at, updated_at) values (%s,%s,%s,%s)"
        for fila in cursor_bronze:
            # Delete from the silver table the previous snapshot of the updated data, and save the new version.
            # grading_cat by grading_cat. We expected to have only one active value per country.
            sql_del = "DELETE FROM grading_fees_silver WHERE grading_cat = '{}'".format(fila[0])
            cursor_silver.execute(sql_del)
            cursor_silver.execute(sql_silver, fila)
            connexion.commit()


def _load_updated_data_platform(ti):

    # Recover the message sent by the _last_updated function.
    data_dict = ti.xcom_pull(task_ids=['last_updated_platform'])[0]
    # To access to the bronze table
    cursor_bronze = connexion.cursor()
    # To access to the silver table
    cursor_silver = connexion.cursor()

    # Build the query
    if not data_dict['empty']:
        sql_bronze = "select platform, cost, created_at, updated_at from platform_cost_bronze"
        sql_bronze += " where updated_at > '{}'".format(data_dict['time'])
        sql_bronze += " and updated_at > created_at order by updated_at asc"

        # Recover the data
        cursor_bronze.execute(sql_bronze)

        # saved in the silver table
        sql_silver = "INSERT INTO platform_cost_silver (platform, cost, created_at, updated_at) values (%s,%s,%s,%s)"
        for fila in cursor_bronze:
            sql_del = "DELETE FROM platform_cost_silver WHERE platform = '{}'".format(fila[0])
            cursor_silver.execute(sql_del)
            cursor_silver.execute(sql_silver, fila)
            connexion.commit()


def _load_updated_data_transport(ti):

    # Recover the message sent by the _last_updated function.
    data_dict = ti.xcom_pull(task_ids=['last_updated_transport'])[0]
    # To access to the bronze table
    cursor_bronze = connexion.cursor()
    # To access to the silver table
    cursor_silver = connexion.cursor()

    # We need to execute only if we have data in the silver table
    if not data_dict['empty']:
        # Look for the updated data in the bronze table
        sql_bronze = "select country, transport_cost, created_at, updated_at from transport_cost_bronze"
        sql_bronze += " where updated_at > '{}'".format(data_dict['time'])
        sql_bronze += " and updated_at > created_at order by updated_at asc"

        # Recover the data
        cursor_bronze.execute(sql_bronze)

        # saved in the silver table
        sql_silver = "INSERT INTO transport_cost_silver (country, transport_cost, created_at, updated_at) " \
                     "values (%s,%s,%s,%s)"
        for fila in cursor_bronze:
            # Delete from the silver table the previous snapshot of the updated data, and save the new version.
            # Country by Country. We expected to have only one active value per country.
            sql_del = "DELETE FROM transport_cost_silver WHERE country = '{}'".format(fila[0])
            cursor_silver.execute(sql_del)
            cursor_silver.execute(sql_silver, fila)
            connexion.commit()


def _end_layer():
    connexion.close()


with DAG("fees",
         start_date=datetime(2023, 8, 1),
         schedule_interval="@daily",
         catchup=False) as dag:\

    bronze_layer = EmptyOperator(task_id="bronze_layer")

    last_updated_grading = PythonOperator(
        task_id="last_updated_grading",
        python_callable=_last_updated_grading
    )

    last_updated_platform = PythonOperator(
        task_id="last_updated_platform",
        python_callable=_last_updated_platform
    )

    last_updated_transport = PythonOperator(
        task_id="last_updated_transport",
        python_callable=_last_updated_transport
    )

    silver_layer = EmptyOperator(task_id="silver_layer")

    load_new_data_grading = PythonOperator(
        task_id="load_new_data_grading",
        python_callable=_load_new_data_grading
    )

    load_updated_data_grading = PythonOperator(
        task_id="load_updated_data_grading",
        python_callable=_load_updated_data_grading
    )

    load_new_data_platform = PythonOperator(
        task_id="load_new_data_platform",
        python_callable=_load_new_data_platform
    )

    load_updated_data_platform = PythonOperator(
        task_id="load_updated_data_platform",
        python_callable=_load_updated_data_platform
    )

    load_new_data_transport = PythonOperator(
        task_id="load_new_data_transport",
        python_callable=_load_new_data_transport
    )
    
    load_updated_data_transport = PythonOperator(
        task_id="load_updated_data_transport",
        python_callable=_load_updated_data_transport
    )
    
    end_layer = PythonOperator(
        task_id="end_layer",
        python_callable=_end_layer
    )

    bronze_layer >> [last_updated_grading, last_updated_platform, last_updated_transport] >> silver_layer
    silver_layer >> [load_new_data_grading, load_updated_data_grading, load_new_data_platform, 
                     load_updated_data_platform, load_new_data_transport, load_updated_data_transport] >> end_layer
