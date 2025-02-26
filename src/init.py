import sys
from common.spark_session import SparkSessionInstance
from pipelines.extract import get_open_food_fact_dataset
from common.utils import log_message
from common.config import global_conf
from pipelines.raw_data_infos import RAW_DATA_SCHEMA_OFF
from services.postgres import write_to_postgres


def main():
    res = True

    try:
        spark = SparkSessionInstance.get_instance()
        dataset = get_open_food_fact_dataset()
        # log_message("INFO", f"test: {dataset[:2]}")

        df = spark.createDataFrame(dataset, RAW_DATA_SCHEMA_OFF)
        df.select("id", "product_name", "product_type").show(5)

        if global_conf.get('GENERAL.ENV') == "dev":
            res = write_to_postgres(df, 
                                    global_conf.get("DATASET.DEFAULT_RAW_TABLE"), 
                                    global_conf.get("POSTGRES.DB_POSTGRES_DEFAULT_SCHEMA")
                                    )
        else:
            res = write_to_bq(df, 
                            global_conf.get("GCP.GCP_PROJECT_ID"), 
                            global_conf.get("GCP.GCP_BIGQUERY_DATASET"), 
                            global_conf.get("DATASET.DEFAULT_RAW_TABLE")
                            )

    except Exception as e:
        log_message("ERROR", f"An error occurred during init: {str(e)}")
        res = False

    finally:
        if res: 
            log_message("INFO", f"Init success")
        else:
            log_message("ERROR", f"Init failed")
        SparkSessionInstance.close_instance()
        return int(not res)


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)