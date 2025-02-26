from google.cloud import bigquery
from common.utils import log_message


def write_to_bq(dataframe, gcp_project_id, bigquery_dataset, table_name, write_mode="overwrite"):
    # TODO Write documentation
    
    # Construct full table path
    table_path = f"{gcp_project_id}.{bigquery_dataset}.{table_name}"
    
    try:
        # Initialize BigQuery client
        bq_client = bigquery.Client(project=gcp_project_id)

        log_message("INFO", dataframe.printSchema())
            
        # Configure write options
        write_options = {
            "table": table_path,
            "createDisposition": "CREATE_IF_NEEDED",
            "writeDisposition": "WRITE_TRUNCATE_DATA",
            "writeMethod": "direct"
        }
        
        # Write to BigQuery
        dataframe.write.format("bigquery") \
            .options(**write_options) \
            .mode(write_mode) \
            .save()
            
        return True
        
    except Exception as e:
        print(f"Error writing to BigQuery: {str(e)}")
        return False
    