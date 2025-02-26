from common.spark_session import SparkSessionInstance
from common.utils import log_message
from common.config import global_conf


def write_to_postgres(dataframe, table_name, schema_name):
    # TODO Add comment write_to_postgres
    try:
        log_message("INFO", dataframe.printSchema())

        # dataframe.write \
        #     .format("jdbc") \
        #     .option("url", f"jdbc:postgresql://{global_conf.get('POSTGRES.DB_POSTGRES_HOST')}:{global_conf.get('POSTGRES.DB_POSTGRES_PORT')}/{global_conf.get('POSTGRES.DB_POSTGRES_NAME')}") \
        #     .option("dbtable", f"{schema_name}.{table_name}") \
        #     .option("user", f"{global_conf.get('POSTGRES.DB_POSTGRES_USER')}") \
        #     .option("password", f"{global_conf.get('POSTGRES.DB_POSTGRES_PASSWORD')}") \
        #     .option("truncate", "true") \
        #     .save()

        dataframe.write.jdbc(url=SparkSessionInstance.get_jdbc_url(), table=f"{schema_name}.{table_name}", mode="overwrite", properties=SparkSessionInstance.get_jdbc_properties())
        log_message("INFO", f"SUCCESS - Adding data to the table {schema_name}.{table_name}")
        return True
    except Exception as e:
        log_message("ERROR", f"Runtime error - Adding data to the table {schema_name}.{table_name} : {e}")
        return False

