resource "azurerm_storage_blob" "artifact_readme" {
  name                   = format("%s/README.md", local.artifact_root)
  storage_account_name   = var.poc_storage_account
  storage_container_name = var.poc_container_name
  type                   = "Block"
  source_content         = "This is a README file for the artifacts directory."
}

resource "azurerm_storage_blob" "charts_pipeline_jar" {
  name                   = local.product_jar_blob_path
  storage_account_name   = var.poc_storage_account
  storage_container_name = var.poc_container_name
  type                   = "Block"
  source                 = "../app/build/libs/spotify-charts-0.0.1.jar"
}

resource "azurerm_storage_blob" "csv_example" {
  name                   = format("%s/file_1.csv", local.scd_landing_path)
  storage_account_name   = var.poc_storage_account
  storage_container_name = var.poc_container_name
  type                   = "Block"
  source                 = "../app/src/test/resources/RawSpotifyChartData/file_1.csv"
}
