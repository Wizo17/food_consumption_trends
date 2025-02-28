from pyspark.sql import SparkSession
from common.config import global_conf
from common.utils import log_message


class SparkSessionInstance:
    _instance = None
    _jdbc_url = f"jdbc:postgresql://{global_conf.get('POSTGRES.DB_POSTGRES_HOST')}:{global_conf.get('POSTGRES.DB_POSTGRES_PORT')}/{global_conf.get('POSTGRES.DB_POSTGRES_NAME')}"
    _jdbc_properties = {
        "user": f"{global_conf.get('POSTGRES.DB_POSTGRES_USER')}",
        "password": f"{global_conf.get('POSTGRES.DB_POSTGRES_PASSWORD')}",
        "driver": "org.postgresql.Driver"
    }

    @staticmethod
    def get_instance():
        if SparkSessionInstance._instance is None:
            if global_conf.get('GENERAL.ENV') == "dev":
                SparkSessionInstance._instance = SparkSession.builder \
                                                    .appName(f"GLOBAL: {global_conf.get('GENERAL.APP_NAME')}") \
                                                    .config("spark.driver.host", global_conf.get('GENERAL.SPARK_HOST')) \
                                                    .config("spark.driver.extraClassPath", global_conf.get('GENERAL.SPARK_JDBC_PATH')) \
                                                    .config("spark.executor.extraClassPath", global_conf.get('GENERAL.SPARK_JDBC_PATH')) \
                                                    .config("spark.files.cleanupTime", "0") \
                                                    .config("spark.shuffle.service.enabled", "false") \
                                                    .config("spark.sql.debug.maxToStringFields", "1000") \
                                                    .config("spark.driver.memory", global_conf.get('GENERAL.SPARK_DRIVER_MEMORY')) \
                                                    .config("spark.executor.memory", global_conf.get('GENERAL.SPARK_EXECUTOR_MEMORY')) \
                                                    .master("local[*]") \
                                                    .getOrCreate()
            else:
                SparkSessionInstance._instance = SparkSession.builder \
                                                    .appName(f"GLOBAL: {global_conf.get('GENERAL.APP_NAME')}") \
                                                    .getOrCreate()
        
        log_message("INFO", f"SparkSession started with version: {SparkSessionInstance._instance.version}")
        return SparkSessionInstance._instance
    
    @staticmethod
    def close_instance():
        if SparkSessionInstance._instance is not None:
            SparkSessionInstance._instance.stop()
            SparkSessionInstance._instance = None 
            log_message("INFO", f"SparkSession closed")    

    @staticmethod
    def get_jdbc_url():
        return SparkSessionInstance._jdbc_url
    
    @staticmethod
    def get_jdbc_properties():
        return SparkSessionInstance._jdbc_properties

