# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

output "key_vault_name" {
  description = "The name of the key vault."
  value       = azurerm_key_vault.default.name
}

output "machine_learning_workspace_name" {
  description = "The name of the machine learning workspace."
  value       = azurerm_machine_learning_workspace.default.name
}

output "ml_compute_cluster_uami_client_id" {
  description = "The client id associated with ml compute cluster uami."
  value       = azurerm_user_assigned_identity.ml_compute_cluster.client_id
}

output "ml_compute_cluster_data_preparation_name" {
  description = "The name of the machine learning compute cluster (data preparation)."
  value       = local.ml_compute_cluster_data_preparation_name
}

output "ml_compute_cluster_training_name" {
  description = "The name of the machine learning compute cluster (training)."
  value       = local.ml_compute_cluster_training_name
}

output "ml_compute_cluster_inference_name" {
  description = "The name of the machine learning compute cluster (inference)."
  value       = local.ml_compute_cluster_inference_name
}

output "ml_datastore_name" {
  description = "The name of the data store in ml workspace."
  value       = var.ml_datastore_name
}
