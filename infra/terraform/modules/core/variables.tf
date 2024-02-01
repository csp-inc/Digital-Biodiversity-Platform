# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

variable "name" {
  description = "The application name, used to name all resources."
  type        = string
}

variable "workload" {
  description = "The workload name, used to name resource group."
  type        = string
  default     = "core"
}

variable "environment" {
  description = "The environment name, used to name all resources."
  type        = string
}

variable "location" {
  description = "The azure region targeted."
  type        = string
}

variable "uid" {
  description = "The unique identifier, used to name all resources."
  type        = string
}

variable "resource_group_name" {
  description = "The resource group name."
  type        = string
}

variable "subnet_compute_id" {
  description = "The subnet resource id (compute)."
  type        = string
}

variable "key_vault_sku_name" {
  description = "The SKU name of the key vault."
  type        = string
}

variable "storage_account_tier" {
  description = "The tier to use for this storage account (default)."
  type        = string
}

variable "storage_account_replication_type" {
  description = "The type of replication to use for this storage account (default)."
  type        = string
}

variable "data_storage_account_tier" {
  description = "The tier to use for this storage account (data storage)."
  type        = string
}

variable "data_storage_account_replication_type" {
  description = "The type of replication to use for this storage account (data storage)."
  type        = string
}

variable "container_registry_sku_name" {
  description = "The SKU name of the container registry."
  type        = string
}

variable "ml_datastore_name" {
  description = "The name of the data store in ml workspace."
  type        = string
  default     = "datablobstore"
}

variable "ml_compute_cluster_data_preparation_prefix" {
  description = "The prefix name for this machine learning computer cluster (data preparation cluster)."
  type        = string
  default     = "clu-data-preparation"
}

variable "ml_compute_cluster_data_preparation_vm_priority" {
  description = "The priority of the virtual machine (data preparation cluster)."
  type        = string
}

variable "ml_compute_cluster_data_preparation_vm_size" {
  description = "The size of the virtual machine (data preparation cluster)."
  type        = string
}

variable "ml_compute_cluster_data_preparation_min_node_count" {
  description = "The minimum number of nodes to use on the cluster (data preparation cluster)."
  type        = number
}

variable "ml_compute_cluster_data_preparation_max_node_count" {
  description = "The maximum number of nodes to use on the cluster (data preparation cluster)."
  type        = number
}

variable "ml_compute_cluster_data_preparation_scale_down_nodes_after_idle_duration" {
  description = "The node idle time before scale down: defines the time until the compute is shutdown when it has gone into idle state (data preparation cluster)."
  type        = string
}

variable "ml_compute_cluster_training_prefix" {
  description = "The prefix name for this machine learning computer cluster (training cluster)."
  type        = string
  default     = "clu-training"
}

variable "ml_compute_cluster_training_vm_priority" {
  description = "The priority of the virtual machine (training cluster)."
  type        = string
}

variable "ml_compute_cluster_training_vm_size" {
  description = "The size of the virtual machine (training cluster)."
  type        = string
}

variable "ml_compute_cluster_training_min_node_count" {
  description = "The minimum number of nodes to use on the cluster (training cluster)."
  type        = number
}

variable "ml_compute_cluster_training_max_node_count" {
  description = "The maximum number of nodes to use on the cluster (training cluster)."
  type        = number
}

variable "ml_compute_cluster_training_scale_down_nodes_after_idle_duration" {
  description = "The node idle time before scale down: defines the time until the compute is shutdown when it has gone into idle state (training cluster)."
  type        = string
}

variable "ml_compute_cluster_inference_prefix" {
  description = "The prefix name for this machine learning computer cluster (inference cluster)."
  type        = string
  default     = "clu-inference"
}

variable "ml_compute_cluster_inference_vm_priority" {
  description = "The priority of the virtual machine (inference cluster)."
  type        = string
}

variable "ml_compute_cluster_inference_vm_size" {
  description = "The size of the virtual machine (inference cluster)."
  type        = string
}

variable "ml_compute_cluster_inference_min_node_count" {
  description = "The minimum number of nodes to use on the cluster (inference cluster)."
  type        = number
}

variable "ml_compute_cluster_inference_max_node_count" {
  description = "The maximum number of nodes to use on the cluster (inference cluster)."
  type        = number
}

variable "ml_compute_cluster_inference_scale_down_nodes_after_idle_duration" {
  description = "The node idle time before scale down: defines the time until the compute is shutdown when it has gone into idle state (inference cluster)."
  type        = string
}
