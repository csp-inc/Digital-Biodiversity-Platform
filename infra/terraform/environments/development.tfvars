#--------------------------------------------------------------
# General
#--------------------------------------------------------------
name        = "ebio"
environment = "dev"
location    = "westeurope"

#--------------------------------------------------------------
# Network
#--------------------------------------------------------------
network_resource_group_name           = "ZWE-EBIO-NET-RG"
network_virtual_network_address_space = ["10.1.0.0/24"] # /24: 256 adresses
network_compute_subnet_address_space  = ["10.1.0.0/26"] # /26:  64 adresses

#--------------------------------------------------------------
# Core
#--------------------------------------------------------------
core_resource_group_name                                                      = "ZWE-EBIO-DEV-RG"
core_key_vault_sku_name                                                       = "premium"
core_storage_account_tier                                                     = "Standard"
core_storage_account_replication_type                                         = "ZRS"
core_data_storage_account_tier                                                = "Premium"
core_data_storage_account_replication_type                                    = "ZRS"
core_container_registry_sku_name                                              = "Standard"
core_ml_compute_cluster_data_preparation_vm_priority                          = "Dedicated"
core_ml_compute_cluster_data_preparation_vm_size                              = "Standard_E8_v3"
core_ml_compute_cluster_data_preparation_min_node_count                       = 0
core_ml_compute_cluster_data_preparation_max_node_count                       = 10
core_ml_compute_cluster_data_preparation_scale_down_nodes_after_idle_duration = "PT10M"
core_ml_compute_cluster_training_vm_priority                                  = "Dedicated"
core_ml_compute_cluster_training_vm_size                                      = "Standard_DS3_v2"
core_ml_compute_cluster_training_min_node_count                               = 0
core_ml_compute_cluster_training_max_node_count                               = 6
core_ml_compute_cluster_training_scale_down_nodes_after_idle_duration         = "PT10M"
core_ml_compute_cluster_inference_vm_priority                                 = "Dedicated"
core_ml_compute_cluster_inference_vm_size                                     = "Standard_DS3_v2"
core_ml_compute_cluster_inference_min_node_count                              = 0
core_ml_compute_cluster_inference_max_node_count                              = 6
core_ml_compute_cluster_inference_scale_down_nodes_after_idle_duration        = "PT10M"
