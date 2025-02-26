#!/bin/bash

PROJECT_ID=${PROJECT_ID:-"analytics-trafic-idfm"}
MAINLAND=eu
REGION=europe-west9
PROJECT_NUMBER=221748092362
CLUSTER_NAME="cluster-spark-analytics"
BUCKET_NAME="food_consumption_trends"


gcloud config set project $PROJECT_ID
gcloud config set dataproc/region $REGION
gcloud config set compute/region $REGION

############################################ CLoud Storage Bucket ############################################
gsutil mb gs://$BUCKET_NAME/
gsutil mb gs://$BUCKET_NAME/source_code/



############################################ BigQuery ############################################
bq mk --location=EU "$BUCKET_NAME"


############################################ Rights ############################################
gcloud projects add-iam-policy-binding analytics-trafic-idfm \
  --member="serviceAccount:$PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
  --role="roles/dataproc.admin"



############################################ Git ############################################
git clone https://github.com/Wizo17/food_consumption_trends.git



############################################ Copy file ############################################
cp food_consumption_trends
cp .env_prod .env
rm -f etl_source_code.zip
cd src
zip -r ../etl_source_code.zip *
cd ..
zip -g etl_source_code.zip .env
zip -g etl_source_code.zip setup.py

gsutil cp -r etl_source_code.zip gs://$BUCKET_NAME/source_code/
gsutil cp -r requirements.txt gs://$BUCKET_NAME/source_code/
gsutil cp -r .env gs://$BUCKET_NAME/source_code/
gsutil cp -r gcp/init_create_dataproc_cluster.sh gs://$BUCKET_NAME/source_code/


############################################ Create Dataproc cluster ############################################

 gcloud dataproc clusters create $CLUSTER_NAME \
        --enable-component-gateway \
        --region $REGION \
        --master-machine-type e2-standard-2 \
        --master-boot-disk-type pd-balanced \
        --master-boot-disk-size 50 \
        --num-workers 2 \
        --worker-machine-type n2-standard-4 \
        --worker-boot-disk-type pd-balanced \
        --worker-boot-disk-size 200 \
        --image-version 2.1-debian11 \
        --optional-components JUPYTER,DOCKER \
        --initialization-actions "gs://$BUCKET_NAME/source_code/init_create_dataproc_cluster.sh" \
        --project $PROJECT_ID


############################################ Run init job ############################################

gcloud dataproc jobs submit pyspark \
        --cluster "$CLUSTER_NAME" \
        --region "$REGION" \
        --py-files "gs://analytics_trafic_idfm/source_code/etl_source_code.zip" \
        --files "gs://analytics_trafic_idfm/source_code/.env" \
        --format='value(reference.jobId)' \
        src/init.py


############################################ Run etl job ############################################


gcloud dataproc jobs submit pyspark \
        --cluster "$CLUSTER_NAME" \
        --region "$REGION" \
        --py-files "gs://analytics_trafic_idfm/source_code/etl_source_code.zip" \
        --files "gs://analytics_trafic_idfm/source_code/.env" \
        --format='value(reference.jobId)' \
        src/main.py

