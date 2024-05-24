terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "3.97.1"
    }
    databricks = {
      source = "databricks/databricks"
      version = "1.38.0"
    }
  }
}

provider "azurerm" {
  features {}
  skip_provider_registration = true
}

#provider "databricks" {
#  azure_workspace_resource_id = module.azure-databricks-workspace.databricks_workspace_resource_id
#  host = module.azure-databricks-workspace.databricks_workspace_url
#  auth_type = "azure-cli"
#}
