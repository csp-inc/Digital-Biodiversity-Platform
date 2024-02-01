# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

output "core_subscription_id" {
  description = "The azure subscription identifier."
  value       = data.azurerm_subscription.current.subscription_id
}

output "core_resource_group_name" {
  description = "The core resource group name."
  value       = var.core_resource_group_name
}

output "core_key_vault_name" {
  description = "The name of the key vault."
  value       = module.core.key_vault_name
}

output "core_platform_uid" {
  description = "The uid of the deployed platform."
  value       = random_string.random.id
}

output "core_machine_learning_workspace_name" {
  description = "The name of the machine learning workspace."
  value       = module.core.machine_learning_workspace_name
}

output "core_ml_compute_cluster_uami_client_id" {
  description = "The client id associated with ml compute cluster uami."
  value       = module.core.ml_compute_cluster_uami_client_id
}

output "core_ml_compute_cluster_data_preparation_name" {
  description = "The name of the machine learning compute cluster (data preparation)."
  value       = module.core.ml_compute_cluster_data_preparation_name
}

output "core_ml_compute_cluster_data_preparation_max_node_count" {
  description = "The maximum number of nodes to use on the cluster (data preparation)."
  value       = var.core_ml_compute_cluster_data_preparation_max_node_count
}

output "core_ml_compute_cluster_training_name" {
  description = "The name of the machine learning compute cluster (training)."
  value       = module.core.ml_compute_cluster_training_name
}

output "core_ml_compute_cluster_training_max_node_count" {
  description = "The maximum number of nodes to use on the cluster (training)."
  value       = var.core_ml_compute_cluster_training_max_node_count
}

output "core_ml_compute_cluster_inference_name" {
  description = "The name of the machine learning compute cluster (inference)."
  value       = module.core.ml_compute_cluster_inference_name
}

output "core_ml_compute_cluster_inference_max_node_count" {
  description = "The maximum number of nodes to use on the cluster (inference)."
  value       = var.core_ml_compute_cluster_inference_max_node_count
}

output "core_ml_datastore_name" {
  description = "The name of the data store in ml workspace."
  value       = module.core.ml_datastore_name
}
