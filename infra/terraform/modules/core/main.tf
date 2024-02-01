# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

terraform {
  required_version = ">=1.3.6"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "=3.82.0"
    }
  }
}

locals {
  resource_version                         = "${var.name}-${var.environment}-${var.uid}"
  resource_version_compact                 = "${var.name}${var.environment}${var.uid}"
  ml_compute_cluster_data_preparation_name = "${var.ml_compute_cluster_data_preparation_prefix}-${var.uid}"
  ml_compute_cluster_training_name         = "${var.ml_compute_cluster_training_prefix}-${var.uid}"
  ml_compute_cluster_inference_name        = "${var.ml_compute_cluster_inference_prefix}-${var.uid}"
  required_tags = {
    name        = var.name
    environment = var.environment
    uid         = var.uid
    workload    = var.workload
  }
}

#--------------------------------------------------------------
# Fundamentals
#--------------------------------------------------------------

# create resource group
resource "azurerm_resource_group" "default" {
  name     = var.resource_group_name
  location = var.location
  tags     = local.required_tags
}

# create log analytics workspace
resource "azurerm_log_analytics_workspace" "default" {
  name                = "log-${local.resource_version}"
  location            = azurerm_resource_group.default.location
  resource_group_name = azurerm_resource_group.default.name
  retention_in_days   = 30
}

# create application insights
resource "azurerm_application_insights" "default" {
  name                = "appi-${local.resource_version}"
  location            = azurerm_resource_group.default.location
  resource_group_name = azurerm_resource_group.default.name
  workspace_id        = azurerm_log_analytics_workspace.default.id
  application_type    = "web"
  tags                = local.required_tags
}

# create key vault
resource "azurerm_key_vault" "default" {
  name                     = "kv-${local.resource_version}"
  location                 = azurerm_resource_group.default.location
  resource_group_name      = azurerm_resource_group.default.name
  tenant_id                = data.azuread_client_config.current.tenant_id
  sku_name                 = var.key_vault_sku_name
  purge_protection_enabled = true
  tags                     = local.required_tags
}

# add key vault access policy (build agent)
resource "azurerm_key_vault_access_policy" "agent" {
  key_vault_id = azurerm_key_vault.default.id
  tenant_id    = data.azuread_client_config.current.tenant_id
  object_id    = data.azuread_client_config.current.object_id

  secret_permissions = [
    "Get", "List", "Set", "Delete", "Recover"
  ]
}

# add key vault access policy (uami ml compute cluster)
resource "azurerm_key_vault_access_policy" "ml_compute_cluster" {
  key_vault_id = azurerm_key_vault.default.id
  tenant_id    = azurerm_user_assigned_identity.ml_compute_cluster.tenant_id
  object_id    = azurerm_user_assigned_identity.ml_compute_cluster.principal_id

  secret_permissions = [
    "Get", "List"
  ]
}

# add secret (application insights connection string)
resource "azurerm_key_vault_secret" "application_insights_connection_string" {
  key_vault_id = azurerm_key_vault.default.id
  name         = "APPLICATIONINSIGHTS-CONNECTION-STRING"
  value        = azurerm_application_insights.default.connection_string

  depends_on = [
    azurerm_key_vault_access_policy.agent
  ]
}

# create storage account
resource "azurerm_storage_account" "default" {
  name                            = "st${local.resource_version_compact}"
  location                        = azurerm_resource_group.default.location
  resource_group_name             = azurerm_resource_group.default.name
  account_tier                    = var.storage_account_tier
  account_replication_type        = var.storage_account_replication_type
  allow_nested_items_to_be_public = false
  tags                            = local.required_tags
}

# create container registry
resource "azurerm_container_registry" "default" {
  name                          = "cr${local.resource_version_compact}"
  location                      = azurerm_resource_group.default.location
  resource_group_name           = azurerm_resource_group.default.name
  sku                           = var.container_registry_sku_name
  admin_enabled                 = true
  public_network_access_enabled = true
  tags                          = local.required_tags
}

#--------------------------------------------------------------
# Data Storage
#--------------------------------------------------------------

# create storage account
resource "azurerm_storage_account" "data" {
  name                            = "stdata${local.resource_version_compact}"
  location                        = azurerm_resource_group.default.location
  resource_group_name             = azurerm_resource_group.default.name
  account_kind                    = "BlockBlobStorage"
  account_tier                    = var.data_storage_account_tier
  account_replication_type        = var.data_storage_account_replication_type
  allow_nested_items_to_be_public = false
  tags                            = local.required_tags
}

# add storage container (data)
resource "azurerm_storage_container" "data" {
  name                  = "data"
  storage_account_name  = azurerm_storage_account.data.name
  container_access_type = "private"
}

#--------------------------------------------------------------
# Azure Machine Learning
#--------------------------------------------------------------

# create machine learning workspace
resource "azurerm_machine_learning_workspace" "default" {
  name                    = "mlw-${local.resource_version}"
  location                = azurerm_resource_group.default.location
  resource_group_name     = azurerm_resource_group.default.name
  application_insights_id = azurerm_application_insights.default.id
  key_vault_id            = azurerm_key_vault.default.id
  storage_account_id      = azurerm_storage_account.default.id
  container_registry_id   = azurerm_container_registry.default.id
  tags                    = local.required_tags

  identity {
    type = "SystemAssigned"
  }
}

# define user assigned managed identity
resource "azurerm_user_assigned_identity" "ml_compute_cluster" {
  name                = "uami-ml-cluster-${local.resource_version}"
  location            = azurerm_resource_group.default.location
  resource_group_name = azurerm_resource_group.default.name
}

# add datastore to ml workspace
resource "azurerm_machine_learning_datastore_blobstorage" "ml_datastore" {
  name                 = var.ml_datastore_name
  workspace_id         = azurerm_machine_learning_workspace.default.id
  storage_container_id = azurerm_storage_container.data.resource_manager_id
  account_key          = azurerm_storage_account.data.primary_access_key
}

# create machine learning compute cluster (data preparation)
resource "azurerm_machine_learning_compute_cluster" "ml_compute_cluster_data_preparation" {
  name                          = local.ml_compute_cluster_data_preparation_name
  location                      = azurerm_resource_group.default.location
  vm_priority                   = var.ml_compute_cluster_data_preparation_vm_priority
  vm_size                       = upper(var.ml_compute_cluster_data_preparation_vm_size)
  machine_learning_workspace_id = azurerm_machine_learning_workspace.default.id
  subnet_resource_id            = var.subnet_compute_id

  scale_settings {
    min_node_count                       = var.ml_compute_cluster_data_preparation_min_node_count
    max_node_count                       = var.ml_compute_cluster_data_preparation_max_node_count
    scale_down_nodes_after_idle_duration = var.ml_compute_cluster_data_preparation_scale_down_nodes_after_idle_duration
  }

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.ml_compute_cluster.id]
  }
}

# create machine learning compute cluster (training)
resource "azurerm_machine_learning_compute_cluster" "ml_compute_cluster_training" {
  name                          = local.ml_compute_cluster_training_name
  location                      = azurerm_resource_group.default.location
  vm_priority                   = var.ml_compute_cluster_training_vm_priority
  vm_size                       = upper(var.ml_compute_cluster_training_vm_size)
  machine_learning_workspace_id = azurerm_machine_learning_workspace.default.id
  subnet_resource_id            = var.subnet_compute_id

  scale_settings {
    min_node_count                       = var.ml_compute_cluster_training_min_node_count
    max_node_count                       = var.ml_compute_cluster_training_max_node_count
    scale_down_nodes_after_idle_duration = var.ml_compute_cluster_training_scale_down_nodes_after_idle_duration
  }

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.ml_compute_cluster.id]
  }
}

# create machine learning compute cluster (inference)
resource "azurerm_machine_learning_compute_cluster" "ml_compute_cluster_inference" {
  name                          = local.ml_compute_cluster_inference_name
  location                      = azurerm_resource_group.default.location
  vm_priority                   = var.ml_compute_cluster_inference_vm_priority
  vm_size                       = upper(var.ml_compute_cluster_inference_vm_size)
  machine_learning_workspace_id = azurerm_machine_learning_workspace.default.id
  subnet_resource_id            = var.subnet_compute_id

  scale_settings {
    min_node_count                       = var.ml_compute_cluster_inference_min_node_count
    max_node_count                       = var.ml_compute_cluster_inference_max_node_count
    scale_down_nodes_after_idle_duration = var.ml_compute_cluster_inference_scale_down_nodes_after_idle_duration
  }

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.ml_compute_cluster.id]
  }
}

# assign role to user assigned managed identity to access/invoke batch endpoints
resource "azurerm_role_assignment" "workspace" {
  scope                = azurerm_machine_learning_workspace.default.id
  role_definition_name = "Azure AI Developer"
  principal_id         = azurerm_user_assigned_identity.ml_compute_cluster.principal_id
}
