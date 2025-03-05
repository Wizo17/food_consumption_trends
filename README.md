# Food consumption and nutritional trends

The project involves analysis for ood consumption and nutritional trends with **BigQueryML**, Python, Pyspark, BigQuery, Power BI, Looker Studio.



## Build with

The project uses:
* [Python](https://www.python.org/)
* [PySpark, SparkSQL](https://spark.apache.org/docs/latest/api/python/index.html)
* [Google Cloud Platform (Dataproc, BigQueryML)](https://cloud.google.com/?hl=fr)
* [Shell script](https://www.shellscript.sh/)
* [Power BI](https://www.microsoft.com/fr-fr/power-platform/products/power-bi) or [Looker Studio](https://lookerstudio.google.com/)

Ressources uses:
* [Open Food Fact](https://fr.openfoodfacts.org/data)



## How it works
* Launch of an init with <u>PySpark</u> to retrieve Open Food Fact data.
* ETL pipeline launched with <u>PySpark</u> to transform data.
* Feed <u>BigQuery</u> tables after the ETL process.
* Building Machine Learning models with <u>BigQueryML</u>.
* Dashboard construction.


## Install

You need an GCP account.

### Requirements

* Python
* Spark
* GCP Account (cloud)
* Git
* BI tool (Power BI or Google Looker Studio)


### GCP

0. **!!!Be carreful!!!**, If you don't include enough data, the statistics may be distorted.
1. Have a GCP account or create an account with a free trial product.
2. Create a new project.
3. Use the console to create elements.
4. Follow script instructions [help_cmd.sh](gcp/help_cmd.sh) to create only your own elements.
5. **!!!Attention!!!**, you don't need to run the whole script and you need to update the commands with your own information.
6. [transform_off_raw_data.py](gcp/bigquery/transform_off_raw_data.py) contains the code for a bigquery stored procedure.
7. You can do all the data analysis you want with the data.
8. Sample queries can be found in the gcp/bigquery folder.
9. A sample report can be found [here](gcp/report/Analysis_food_sample.pdf)
10. For the ML part, examples of queries for creating a model can be found [here](gcp/bigquery/bigquery_ml.sql).
11. Once you have created the models, you can predict the clusters and compare them with the nutriscore.


## Versions
**LTS :** [1.0](https://github.com/Wizo17/food_consumption_trends.git)

## Auteurs
* [@wizo17](https://github.com/Wizo17)

## License
This project is licensed by  ``MIT`` - see [LICENSE](LICENSE) for more informations.

