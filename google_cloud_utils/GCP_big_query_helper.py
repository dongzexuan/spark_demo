import datetime
from google.cloud import bigquery
from pytz import timezone


def check_exist_table(dataset_id, table_id):
    """
    Method that check a table in the dataset - will create the table if not exist
    :param dataset_id: dataset
    :table_id: table name
    """
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)

    tables = [x.table_id for x in list(client.list_tables(dataset_ref))]

    # print('Checking on the bigquery table: ')
    # if table_id in tables:
    #     # print('Table already exists: {}.{}'.format(dataset_id, table_id))
    #     return True
    # else:
    #     # print('Table does not exist: {}.{}'.format(dataset_id, table_id))
    #     return False
    return bool(table_id in tables)


def create_bq_table(dataset_id, table_id, schema):
    """
    Method that check a table in the dataset - will create the table if not exist
    :param dataset_id: dataset
    :table_id: table name
    """
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)

    try:
        table_ref = dataset_ref.table(table_id)
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        return True
    except Exception:
        return False


def bq_add_record(
        dataset_id,
        table_id,
        job_id,
        username,
        status,
        filename,
        description,
        basin_dir,
        workflow_type):
    """
    Method that connect to bigquery and add record to a table in the dataset
    :param dataset_id: dataset
    :table_id: table name
    :job_id: job id
    :username: user name
    :status: job status (Processing/Finished)
    """

    # Check the table_id to see if it exists in the dataset,
    # if not, this will create it
    if not check_exist_table(dataset_id, table_id):
        print("Table does not exist - will create the table: {}.{}".format(
                dataset_id, table_id))
        schema = [
            bigquery.SchemaField('job_id', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('username', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('timestamp', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('status', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField("filename", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("basin_dir", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("workflow_type", "STRING", mode="NULLABLE")
        ]
        create_bq_table(dataset_id, table_id, schema)

    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)

    table_ref = dataset_ref.table(table_id)
    table = client.get_table(table_ref)

    # Had to hard code timezone for properly retrieving time in docker
    # (docker by fault uses UTC time zone)
    central = timezone("US/Central")
    str_time = datetime.datetime.now(central).strftime("%Y-%m-%d %H:%M:%S")

    rows_to_insert = [
        (str(job_id),
         username,
         str_time,
         status,
         filename,
         description,
         basin_dir,
         workflow_type)
    ]
    # print(rows_to_insert)

    errors = client.insert_rows(table, rows_to_insert)
    assert errors == []
