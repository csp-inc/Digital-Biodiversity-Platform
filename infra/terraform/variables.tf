# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

variable "name" {
  description = "The application name, used to name all resources."
  type        = string
}

variable "environment" {
  description = "The environment name, used to name all resources."
  type        = string

  validation {
    condition     = contains(["dev", "rel"], var.environment)
    error_message = "Invalid input for \"environment\", options: \"dev\", \"rel\"."
  }
}

variable "location" {
  description = "The azure region targeted."
  type        = string
}

variable "network_resource_group_name" {
  description = "The network resource group name."
  type        = string
}

variable "network_virtual_network_address_space" {
  description = "The virtual network address space (compute)."
  type        = list(string)
}

variable "network_compute_subnet_address_space" {
  description = "The subnet address space (compute)."
  type        = list(string)
}

variable "core_key_vault_sku_name" {
  description = "The SKU name of the key vault."
  type        = string

  validation {
    condition     = contains(["standard", "premium"], var.core_key_vault_sku_name)
    error_message = "Invalid input for \"core_key_vault_sku_name\", options: \"standard\", \"premium\"."
  }
}

variable "core_resource_group_name" {
  description = "The core resource group name."
  type        = string
}

variable "core_storage_account_tier" {
  description = "The tier to use for this storage account (default)."
  type        = string

  validation {
    condition     = contains(["Standard", "Premium"], var.core_storage_account_tier)
    error_message = "Invalid input for \"core_storage_account_tier\", options: \"Standard\", \"Premium\"."
  }
}

variable "core_storage_account_replication_type" {
  description = "The type of replication to use for this storage account (default)."
  type        = string

  validation {
    condition     = contains(["LRS", "GRS", "RAGRS", "ZRS", "GZRS", "RAGZRS"], var.core_storage_account_replication_type)
    error_message = "Invalid input for \"core_storage_account_replication_type\", options: \"LRS\", \"GRS\", \"RAGRS\", \"ZRS\", \"GZRS\", \"RAGZRS\"."
  }
}

variable "core_data_storage_account_tier" {
  description = "The tier to use for this storage account (data storage)."
  type        = string

  validation {
    condition     = contains(["Standard", "Premium"], var.core_data_storage_account_tier)
    error_message = "Invalid input for \"core_data_storage_account_tier\", options: \"Standard\", \"Premium\"."
  }
}

variable "core_data_storage_account_replication_type" {
  description = "The type of replication to use for this storage account (data storage)."
  type        = string

  validation {
    condition     = contains(["LRS", "GRS", "RAGRS", "ZRS", "GZRS", "RAGZRS"], var.core_data_storage_account_replication_type)
    error_message = "Invalid input for \"core_data_storage_account_replication_type\", options: \"LRS\", \"GRS\", \"RAGRS\", \"ZRS\", \"GZRS\", \"RAGZRS\"."
  }
}

variable "core_container_registry_sku_name" {
  description = "The SKU name of the container registry."
  type        = string

  validation {
    condition     = contains(["Basic", "Standard", "Premium"], var.core_container_registry_sku_name)
    error_message = "Invalid input for \"core_container_registry_sku_name\", options: \"Basic\", \"Standard\", \"Premium\"."
  }
}

variable "core_ml_compute_cluster_data_preparation_vm_priority" {
  description = "The priority of the virtual machine (data preparation cluster)."
  type        = string

  validation {
    condition     = contains(["Dedicated", "LowPriority"], var.core_ml_compute_cluster_data_preparation_vm_priority)
    error_message = "Invalid input for \"core_ml_compute_cluster_data_preparation_vm_priority\", options: \"Dedicated\", \"LowPriority\"."
  }
}

variable "core_ml_compute_cluster_data_preparation_vm_size" {
  description = "The size of the virtual machine (data preparation cluster)."
  type        = string
}

variable "core_ml_compute_cluster_data_preparation_min_node_count" {
  description = "The minimum number of nodes to use on the cluster (data preparation cluster)."
  type        = number
}

variable "core_ml_compute_cluster_data_preparation_max_node_count" {
  description = "The maximum number of nodes to use on the cluster (data preparation cluster)."
  type        = number
}

variable "core_ml_compute_cluster_data_preparation_scale_down_nodes_after_idle_duration" {
  description = "The node idle time before scale down: defines the time until the compute is shutdown when it has gone into idle state (data preparation cluster)."
  type        = string
}

variable "core_ml_compute_cluster_training_vm_priority" {
  description = "The priority of the virtual machine (training cluster)."
  type        = string
}

variable "core_ml_compute_cluster_training_vm_size" {
  description = "The size of the virtual machine (training cluster)."
  type        = string
}

variable "core_ml_compute_cluster_training_min_node_count" {
  description = "The minimum number of nodes to use on the cluster (training cluster)."
  type        = number
}

variable "core_ml_compute_cluster_training_max_node_count" {
  description = "The maximum number of nodes to use on the cluster (training cluster)."
  type        = number
}

variable "core_ml_compute_cluster_training_scale_down_nodes_after_idle_duration" {
  description = "The node idle time before scale down: defines the time until the compute is shutdown when it has gone into idle state (training cluster)."
  type        = string
}

variable "core_ml_compute_cluster_inference_vm_priority" {
  description = "The priority of the virtual machine (inference cluster)."
  type        = string
}

variable "core_ml_compute_cluster_inference_vm_size" {
  description = "The size of the virtual machine (inference cluster)."
  type        = string
}

variable "core_ml_compute_cluster_inference_min_node_count" {
  description = "The minimum number of nodes to use on the cluster (inference cluster)."
  type        = number
}

variable "core_ml_compute_cluster_inference_max_node_count" {
  description = "The maximum number of nodes to use on the cluster (inference cluster)."
  type        = number
}

variable "core_ml_compute_cluster_inference_scale_down_nodes_after_idle_duration" {
  description = "The node idle time before scale down: defines the time until the compute is shutdown when it has gone into idle state (inference cluster)."
  type        = string
}
