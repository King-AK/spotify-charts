variable "poc_storage_account" {}
variable "poc_container_name" {}
variable "poc_resource_group" {}
variable "poc_location" {}


resource "azurerm_storage_blob" "artifact_readme" {
  name                   = "artifacts/README.md"
  storage_account_name   = var.poc_storage_account
  storage_container_name = var.poc_container_name
  type                   = "Block"
  source_content = "This is a README file for the artifacts directory."
}

// TODO add upload of JAR to artifact container w/ confirmation that JAR exists first

// TODO: upload the test directory CSV files to the landing zone
resource "azurerm_storage_blob" "csv_landing_zone_readme" {
  name                   = "landing/spotify-csv-batches"
  storage_account_name   = var.poc_storage_account
  storage_container_name = var.poc_container_name
  type                   = "Block"
  source_content = "This is a README file for the landing zone directory."
}

// TODO - possibly in a separate file - create databricks jobs