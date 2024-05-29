# spotify-charts

## Overview
This project is an example of a data product leveraging a medallion lakehouse architecture with Spark Streaming.

Dataset: [Spotify Charts](https://www.kaggle.com/datasets/sunnykakar/spotify-charts-all-audio-data)
Dataset descripition from Kaggle:
> This is a complete dataset of all the "Top 200" and "Viral 50" charts published globally by Spotify. Spotify publishes a new chart every 2-3 days. This is its entire collection since January 1, 2019. This dataset is a continuation of the Kaggle Dataset: Spotify Charts but contains 29 rows for each row that was populated using the Spotify API. 

## Gradle
Run tests with the following command:
```bash
gradle test
```

Build the project with the following command:
```bash
gradle build
```

## Terraform Deploy
The pipeline can be deployed to Azure using Terraform with the following commands after the project has been set up with the `databricks_poc` repo
```bash
cd terraform
terraform init
terraform apply -auto-approve -var-file=environment/poc.tfvars \
  -var='databricks_workspace_resource_id=<DATABRICKS-WORKSPACE-RESOURCE-ID>' \
  -var='databricks_workspace_url=<DATABRICKS-WORKSPACE-URL>' \
  -var='poc_storage_account_key=<STORAGE-ACCOUNT-KEY>'
```

## Terraform Destroy
The pipeline can be destroyed with the following command:
```bash
terraform destroy -auto-approve -var-file=environment/poc.tfvars \
  -var='databricks_workspace_resource_id=<DATABRICKS-WORKSPACE-RESOURCE-ID>' \
  -var='databricks_workspace_url=<DATABRICKS-WORKSPACE-URL>' \
  -var='poc_storage_account_key=<STORAGE-ACCOUNT-KEY>'
```

## Local Run
The pipeline can be run locally with spark-submit (spark version >=3.5) using the following command:
```bash
bash utils/local-run.sh
```

