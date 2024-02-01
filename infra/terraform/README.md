# Root module (terraform-docs)

## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_terraform"></a> [terraform](#requirement\_terraform) | >=1.3.6 |
| <a name="requirement_azurerm"></a> [azurerm](#requirement\_azurerm) | =3.82.0 |
| <a name="requirement_random"></a> [random](#requirement\_random) | =3.5.1 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_azurerm"></a> [azurerm](#provider\_azurerm) | =3.82.0 |
| <a name="provider_random"></a> [random](#provider\_random) | =3.5.1 |

## Modules

| Name | Source | Version |
|------|--------|---------|
| <a name="module_core"></a> [core](#module\_core) | ./modules/core | n/a |
| <a name="module_network"></a> [network](#module\_network) | ./modules/network | n/a |

## Resources

| Name | Type |
|------|------|
| [random_string.random](https://registry.terraform.io/providers/hashicorp/random/3.5.1/docs/resources/string) | resource |
| [azurerm_subscription.current](https://registry.terraform.io/providers/hashicorp/azurerm/3.82.0/docs/data-sources/subscription) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_core_container_registry_sku_name"></a> [core\_container\_registry\_sku\_name](#input\_core\_container\_registry\_sku\_name) | The SKU name of the container registry. | `string` | n/a | yes |
| <a name="input_core_data_storage_account_replication_type"></a> [core\_data\_storage\_account\_replication\_type](#input\_core\_data\_storage\_account\_replication\_type) | The type of replication to use for this storage account (data storage). | `string` | n/a | yes |
| <a name="input_core_data_storage_account_tier"></a> [core\_data\_storage\_account\_tier](#input\_core\_data\_storage\_account\_tier) | The tier to use for this storage account (data storage). | `string` | n/a | yes |
| <a name="input_core_key_vault_sku_name"></a> [core\_key\_vault\_sku\_name](#input\_core\_key\_vault\_sku\_name) | The SKU name of the key vault. | `string` | n/a | yes |
| <a name="input_core_ml_compute_cluster_data_preparation_max_node_count"></a> [core\_ml\_compute\_cluster\_data\_preparation\_max\_node\_count](#input\_core\_ml\_compute\_cluster\_data\_preparation\_max\_node\_count) | The maximum number of nodes to use on the cluster (data preparation cluster). | `number` | n/a | yes |
| <a name="input_core_ml_compute_cluster_data_preparation_min_node_count"></a> [core\_ml\_compute\_cluster\_data\_preparation\_min\_node\_count](#input\_core\_ml\_compute\_cluster\_data\_preparation\_min\_node\_count) | The minimum number of nodes to use on the cluster (data preparation cluster). | `number` | n/a | yes |
| <a name="input_core_ml_compute_cluster_data_preparation_scale_down_nodes_after_idle_duration"></a> [core\_ml\_compute\_cluster\_data\_preparation\_scale\_down\_nodes\_after\_idle\_duration](#input\_core\_ml\_compute\_cluster\_data\_preparation\_scale\_down\_nodes\_after\_idle\_duration) | The node idle time before scale down: defines the time until the compute is shutdown when it has gone into idle state (data preparation cluster). | `string` | n/a | yes |
| <a name="input_core_ml_compute_cluster_data_preparation_vm_priority"></a> [core\_ml\_compute\_cluster\_data\_preparation\_vm\_priority](#input\_core\_ml\_compute\_cluster\_data\_preparation\_vm\_priority) | The priority of the virtual machine (data preparation cluster). | `string` | n/a | yes |
| <a name="input_core_ml_compute_cluster_data_preparation_vm_size"></a> [core\_ml\_compute\_cluster\_data\_preparation\_vm\_size](#input\_core\_ml\_compute\_cluster\_data\_preparation\_vm\_size) | The size of the virtual machine (data preparation cluster). | `string` | n/a | yes |
| <a name="input_core_ml_compute_cluster_inference_max_node_count"></a> [core\_ml\_compute\_cluster\_inference\_max\_node\_count](#input\_core\_ml\_compute\_cluster\_inference\_max\_node\_count) | The maximum number of nodes to use on the cluster (inference cluster). | `number` | n/a | yes |
| <a name="input_core_ml_compute_cluster_inference_min_node_count"></a> [core\_ml\_compute\_cluster\_inference\_min\_node\_count](#input\_core\_ml\_compute\_cluster\_inference\_min\_node\_count) | The minimum number of nodes to use on the cluster (inference cluster). | `number` | n/a | yes |
| <a name="input_core_ml_compute_cluster_inference_scale_down_nodes_after_idle_duration"></a> [core\_ml\_compute\_cluster\_inference\_scale\_down\_nodes\_after\_idle\_duration](#input\_core\_ml\_compute\_cluster\_inference\_scale\_down\_nodes\_after\_idle\_duration) | The node idle time before scale down: defines the time until the compute is shutdown when it has gone into idle state (inference cluster). | `string` | n/a | yes |
| <a name="input_core_ml_compute_cluster_inference_vm_priority"></a> [core\_ml\_compute\_cluster\_inference\_vm\_priority](#input\_core\_ml\_compute\_cluster\_inference\_vm\_priority) | The priority of the virtual machine (inference cluster). | `string` | n/a | yes |
| <a name="input_core_ml_compute_cluster_inference_vm_size"></a> [core\_ml\_compute\_cluster\_inference\_vm\_size](#input\_core\_ml\_compute\_cluster\_inference\_vm\_size) | The size of the virtual machine (inference cluster). | `string` | n/a | yes |
| <a name="input_core_ml_compute_cluster_training_max_node_count"></a> [core\_ml\_compute\_cluster\_training\_max\_node\_count](#input\_core\_ml\_compute\_cluster\_training\_max\_node\_count) | The maximum number of nodes to use on the cluster (training cluster). | `number` | n/a | yes |
| <a name="input_core_ml_compute_cluster_training_min_node_count"></a> [core\_ml\_compute\_cluster\_training\_min\_node\_count](#input\_core\_ml\_compute\_cluster\_training\_min\_node\_count) | The minimum number of nodes to use on the cluster (training cluster). | `number` | n/a | yes |
| <a name="input_core_ml_compute_cluster_training_scale_down_nodes_after_idle_duration"></a> [core\_ml\_compute\_cluster\_training\_scale\_down\_nodes\_after\_idle\_duration](#input\_core\_ml\_compute\_cluster\_training\_scale\_down\_nodes\_after\_idle\_duration) | The node idle time before scale down: defines the time until the compute is shutdown when it has gone into idle state (training cluster). | `string` | n/a | yes |
| <a name="input_core_ml_compute_cluster_training_vm_priority"></a> [core\_ml\_compute\_cluster\_training\_vm\_priority](#input\_core\_ml\_compute\_cluster\_training\_vm\_priority) | The priority of the virtual machine (training cluster). | `string` | n/a | yes |
| <a name="input_core_ml_compute_cluster_training_vm_size"></a> [core\_ml\_compute\_cluster\_training\_vm\_size](#input\_core\_ml\_compute\_cluster\_training\_vm\_size) | The size of the virtual machine (training cluster). | `string` | n/a | yes |
| <a name="input_core_resource_group_name"></a> [core\_resource\_group\_name](#input\_core\_resource\_group\_name) | The core resource group name. | `string` | n/a | yes |
| <a name="input_core_storage_account_replication_type"></a> [core\_storage\_account\_replication\_type](#input\_core\_storage\_account\_replication\_type) | The type of replication to use for this storage account (default). | `string` | n/a | yes |
| <a name="input_core_storage_account_tier"></a> [core\_storage\_account\_tier](#input\_core\_storage\_account\_tier) | The tier to use for this storage account (default). | `string` | n/a | yes |
| <a name="input_environment"></a> [environment](#input\_environment) | The environment name, used to name all resources. | `string` | n/a | yes |
| <a name="input_location"></a> [location](#input\_location) | The azure region targeted. | `string` | n/a | yes |
| <a name="input_name"></a> [name](#input\_name) | The application name, used to name all resources. | `string` | n/a | yes |
| <a name="input_network_compute_subnet_address_space"></a> [network\_compute\_subnet\_address\_space](#input\_network\_compute\_subnet\_address\_space) | The subnet address space (compute). | `list(string)` | n/a | yes |
| <a name="input_network_resource_group_name"></a> [network\_resource\_group\_name](#input\_network\_resource\_group\_name) | The network resource group name. | `string` | n/a | yes |
| <a name="input_network_virtual_network_address_space"></a> [network\_virtual\_network\_address\_space](#input\_network\_virtual\_network\_address\_space) | The virtual network address space (compute). | `list(string)` | n/a | yes |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_core_key_vault_name"></a> [core\_key\_vault\_name](#output\_core\_key\_vault\_name) | The name of the key vault. |
| <a name="output_core_machine_learning_workspace_name"></a> [core\_machine\_learning\_workspace\_name](#output\_core\_machine\_learning\_workspace\_name) | The name of the machine learning workspace. |
| <a name="output_core_ml_compute_cluster_data_preparation_max_node_count"></a> [core\_ml\_compute\_cluster\_data\_preparation\_max\_node\_count](#output\_core\_ml\_compute\_cluster\_data\_preparation\_max\_node\_count) | The maximum number of nodes to use on the cluster (data preparation). |
| <a name="output_core_ml_compute_cluster_data_preparation_name"></a> [core\_ml\_compute\_cluster\_data\_preparation\_name](#output\_core\_ml\_compute\_cluster\_data\_preparation\_name) | The name of the machine learning compute cluster (data preparation). |
| <a name="output_core_ml_compute_cluster_inference_max_node_count"></a> [core\_ml\_compute\_cluster\_inference\_max\_node\_count](#output\_core\_ml\_compute\_cluster\_inference\_max\_node\_count) | The maximum number of nodes to use on the cluster (inference). |
| <a name="output_core_ml_compute_cluster_inference_name"></a> [core\_ml\_compute\_cluster\_inference\_name](#output\_core\_ml\_compute\_cluster\_inference\_name) | The name of the machine learning compute cluster (inference). |
| <a name="output_core_ml_compute_cluster_training_max_node_count"></a> [core\_ml\_compute\_cluster\_training\_max\_node\_count](#output\_core\_ml\_compute\_cluster\_training\_max\_node\_count) | The maximum number of nodes to use on the cluster (training). |
| <a name="output_core_ml_compute_cluster_training_name"></a> [core\_ml\_compute\_cluster\_training\_name](#output\_core\_ml\_compute\_cluster\_training\_name) | The name of the machine learning compute cluster (training). |
| <a name="output_core_ml_compute_cluster_uami_client_id"></a> [core\_ml\_compute\_cluster\_uami\_client\_id](#output\_core\_ml\_compute\_cluster\_uami\_client\_id) | The client id associated with ml compute cluster uami. |
| <a name="output_core_ml_datastore_name"></a> [core\_ml\_datastore\_name](#output\_core\_ml\_datastore\_name) | The name of the data store in ml workspace. |
| <a name="output_core_platform_uid"></a> [core\_platform\_uid](#output\_core\_platform\_uid) | The uid of the deployed platform. |
| <a name="output_core_resource_group_name"></a> [core\_resource\_group\_name](#output\_core\_resource\_group\_name) | The core resource group name. |
| <a name="output_core_subscription_id"></a> [core\_subscription\_id](#output\_core\_subscription\_id) | The azure subscription identifier. |
