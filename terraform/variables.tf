variable "poc_storage_account" {}
variable "poc_container_name" {}
variable "poc_resource_group" {}
variable "poc_location" {}
variable "databricks_workspace_resource_id" {}
variable "databricks_workspace_url" {}
variable "poc_storage_account_key" {
  type      = string
  sensitive = true
}
