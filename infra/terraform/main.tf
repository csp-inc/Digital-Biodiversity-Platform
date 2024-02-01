# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

terraform {
  required_version = ">=1.3.6"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "=3.82.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "=3.5.1"
    }
  }
}

# START - NEED TO BE COMMENTED IF YOU WORK WITH A LOCAL STATE
terraform {
  backend "azurerm" {}
}
# END

provider "azurerm" {
  features {}
}

resource "random_string" "random" {
  length      = 4
  numeric     = true
  lower       = true
  upper       = false
  special     = false
  min_numeric = 1
}

module "network" {
  source                        = "./modules/network"
  name                          = var.name
  environment                   = var.environment
  location                      = var.location
  uid                           = random_string.random.id
  resource_group_name           = var.network_resource_group_name
  virtual_network_address_space = var.network_virtual_network_address_space
  compute_subnet_address_space  = var.network_compute_subnet_address_space
}

module "core" {
  source                                                                   = "./modules/core"
  name                                                                     = var.name
  environment                                                              = var.environment
  location                                                                 = var.location
  uid                                                                      = random_string.random.id
  resource_group_name                                                      = var.core_resource_group_name
  subnet_compute_id                                                        = module.network.subnet_compute_id
  key_vault_sku_name                                                       = var.core_key_vault_sku_name
  storage_account_tier                                                     = var.core_storage_account_tier
  storage_account_replication_type                                         = var.core_storage_account_replication_type
  data_storage_account_tier                                                = var.core_data_storage_account_tier
  data_storage_account_replication_type                                    = var.core_data_storage_account_replication_type
  container_registry_sku_name                                              = var.core_container_registry_sku_name
  ml_compute_cluster_data_preparation_vm_priority                          = var.core_ml_compute_cluster_data_preparation_vm_priority
  ml_compute_cluster_data_preparation_vm_size                              = var.core_ml_compute_cluster_data_preparation_vm_size
  ml_compute_cluster_data_preparation_min_node_count                       = var.core_ml_compute_cluster_data_preparation_min_node_count
  ml_compute_cluster_data_preparation_max_node_count                       = var.core_ml_compute_cluster_data_preparation_max_node_count
  ml_compute_cluster_data_preparation_scale_down_nodes_after_idle_duration = var.core_ml_compute_cluster_data_preparation_scale_down_nodes_after_idle_duration
  ml_compute_cluster_training_vm_priority                                  = var.core_ml_compute_cluster_training_vm_priority
  ml_compute_cluster_training_vm_size                                      = var.core_ml_compute_cluster_training_vm_size
  ml_compute_cluster_training_min_node_count                               = var.core_ml_compute_cluster_training_min_node_count
  ml_compute_cluster_training_max_node_count                               = var.core_ml_compute_cluster_training_max_node_count
  ml_compute_cluster_training_scale_down_nodes_after_idle_duration         = var.core_ml_compute_cluster_training_scale_down_nodes_after_idle_duration
  ml_compute_cluster_inference_vm_priority                                 = var.core_ml_compute_cluster_inference_vm_priority
  ml_compute_cluster_inference_vm_size                                     = var.core_ml_compute_cluster_inference_vm_size
  ml_compute_cluster_inference_min_node_count                              = var.core_ml_compute_cluster_inference_min_node_count
  ml_compute_cluster_inference_max_node_count                              = var.core_ml_compute_cluster_inference_max_node_count
  ml_compute_cluster_inference_scale_down_nodes_after_idle_duration        = var.core_ml_compute_cluster_inference_scale_down_nodes_after_idle_duration
}
