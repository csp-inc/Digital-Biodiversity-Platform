# Core module (terraform-docs)

## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_terraform"></a> [terraform](#requirement\_terraform) | >=1.3.6 |
| <a name="requirement_azurerm"></a> [azurerm](#requirement\_azurerm) | =3.82.0 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_azuread"></a> [azuread](#provider\_azuread) | n/a |
| <a name="provider_azurerm"></a> [azurerm](#provider\_azurerm) | =3.82.0 |

## Modules

No modules.

## Resources

| Name | Type |
|------|------|
| [azurerm_application_insights.default](https://registry.terraform.io/providers/hashicorp/azurerm/3.82.0/docs/resources/application_insights) | resource |
| [azurerm_container_registry.default](https://registry.terraform.io/providers/hashicorp/azurerm/3.82.0/docs/resources/container_registry) | resource |
| [azurerm_key_vault.default](https://registry.terraform.io/providers/hashicorp/azurerm/3.82.0/docs/resources/key_vault) | resource |
| [azurerm_key_vault_access_policy.agent](https://registry.terraform.io/providers/hashicorp/azurerm/3.82.0/docs/resources/key_vault_access_policy) | resource |
| [azurerm_key_vault_access_policy.ml_compute_cluster](https://registry.terraform.io/providers/hashicorp/azurerm/3.82.0/docs/resources/key_vault_access_policy) | resource |
| [azurerm_key_vault_secret.application_insights_connection_string](https://registry.terraform.io/providers/hashicorp/azurerm/3.82.0/docs/resources/key_vault_secret) | resource |
| [azurerm_log_analytics_workspace.default](https://registry.terraform.io/providers/hashicorp/azurerm/3.82.0/docs/resources/log_analytics_workspace) | resource |
| [azurerm_machine_learning_compute_cluster.ml_compute_cluster_data_preparation](https://registry.terraform.io/providers/hashicorp/azurerm/3.82.0/docs/resources/machine_learning_compute_cluster) | resource |
| [azurerm_machine_learning_compute_cluster.ml_compute_cluster_inference](https://registry.terraform.io/providers/hashicorp/azurerm/3.82.0/docs/resources/machine_learning_compute_cluster) | resource |
| [azurerm_machine_learning_compute_cluster.ml_compute_cluster_training](https://registry.terraform.io/providers/hashicorp/azurerm/3.82.0/docs/resources/machine_learning_compute_cluster) | resource |
| [azurerm_machine_learning_datastore_blobstorage.ml_datastore](https://registry.terraform.io/providers/hashicorp/azurerm/3.82.0/docs/resources/machine_learning_datastore_blobstorage) | resource |
| [azurerm_machine_learning_workspace.default](https://registry.terraform.io/providers/hashicorp/azurerm/3.82.0/docs/resources/machine_learning_workspace) | resource |
| [azurerm_resource_group.default](https://registry.terraform.io/providers/hashicorp/azurerm/3.82.0/docs/resources/resource_group) | resource |
| [azurerm_role_assignment.workspace](https://registry.terraform.io/providers/hashicorp/azurerm/3.82.0/docs/resources/role_assignment) | resource |
| [azurerm_storage_account.data](https://registry.terraform.io/providers/hashicorp/azurerm/3.82.0/docs/resources/storage_account) | resource |
| [azurerm_storage_account.default](https://registry.terraform.io/providers/hashicorp/azurerm/3.82.0/docs/resources/storage_account) | resource |
| [azurerm_storage_container.data](https://registry.terraform.io/providers/hashicorp/azurerm/3.82.0/docs/resources/storage_container) | resource |
| [azurerm_user_assigned_identity.ml_compute_cluster](https://registry.terraform.io/providers/hashicorp/azurerm/3.82.0/docs/resources/user_assigned_identity) | resource |
| [azuread_client_config.current](https://registry.terraform.io/providers/hashicorp/azuread/latest/docs/data-sources/client_config) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_container_registry_sku_name"></a> [container\_registry\_sku\_name](#input\_container\_registry\_sku\_name) | The SKU name of the container registry. | `string` | n/a | yes |
| <a name="input_data_storage_account_replication_type"></a> [data\_storage\_account\_replication\_type](#input\_data\_storage\_account\_replication\_type) | The type of replication to use for this storage account (data storage). | `string` | n/a | yes |
| <a name="input_data_storage_account_tier"></a> [data\_storage\_account\_tier](#input\_data\_storage\_account\_tier) | The tier to use for this storage account (data storage). | `string` | n/a | yes |
| <a name="input_environment"></a> [environment](#input\_environment) | The environment name, used to name all resources. | `string` | n/a | yes |
| <a name="input_key_vault_sku_name"></a> [key\_vault\_sku\_name](#input\_key\_vault\_sku\_name) | The SKU name of the key vault. | `string` | n/a | yes |
| <a name="input_location"></a> [location](#input\_location) | The azure region targeted. | `string` | n/a | yes |
| <a name="input_ml_compute_cluster_data_preparation_max_node_count"></a> [ml\_compute\_cluster\_data\_preparation\_max\_node\_count](#input\_ml\_compute\_cluster\_data\_preparation\_max\_node\_count) | The maximum number of nodes to use on the cluster (data preparation cluster). | `number` | n/a | yes |
| <a name="input_ml_compute_cluster_data_preparation_min_node_count"></a> [ml\_compute\_cluster\_data\_preparation\_min\_node\_count](#input\_ml\_compute\_cluster\_data\_preparation\_min\_node\_count) | The minimum number of nodes to use on the cluster (data preparation cluster). | `number` | n/a | yes |
| <a name="input_ml_compute_cluster_data_preparation_prefix"></a> [ml\_compute\_cluster\_data\_preparation\_prefix](#input\_ml\_compute\_cluster\_data\_preparation\_prefix) | The prefix name for this machine learning computer cluster (data preparation cluster). | `string` | `"clu-data-preparation"` | no |
| <a name="input_ml_compute_cluster_data_preparation_scale_down_nodes_after_idle_duration"></a> [ml\_compute\_cluster\_data\_preparation\_scale\_down\_nodes\_after\_idle\_duration](#input\_ml\_compute\_cluster\_data\_preparation\_scale\_down\_nodes\_after\_idle\_duration) | The node idle time before scale down: defines the time until the compute is shutdown when it has gone into idle state (data preparation cluster). | `string` | n/a | yes |
| <a name="input_ml_compute_cluster_data_preparation_vm_priority"></a> [ml\_compute\_cluster\_data\_preparation\_vm\_priority](#input\_ml\_compute\_cluster\_data\_preparation\_vm\_priority) | The priority of the virtual machine (data preparation cluster). | `string` | n/a | yes |
| <a name="input_ml_compute_cluster_data_preparation_vm_size"></a> [ml\_compute\_cluster\_data\_preparation\_vm\_size](#input\_ml\_compute\_cluster\_data\_preparation\_vm\_size) | The size of the virtual machine (data preparation cluster). | `string` | n/a | yes |
| <a name="input_ml_compute_cluster_inference_max_node_count"></a> [ml\_compute\_cluster\_inference\_max\_node\_count](#input\_ml\_compute\_cluster\_inference\_max\_node\_count) | The maximum number of nodes to use on the cluster (inference cluster). | `number` | n/a | yes |
| <a name="input_ml_compute_cluster_inference_min_node_count"></a> [ml\_compute\_cluster\_inference\_min\_node\_count](#input\_ml\_compute\_cluster\_inference\_min\_node\_count) | The minimum number of nodes to use on the cluster (inference cluster). | `number` | n/a | yes |
| <a name="input_ml_compute_cluster_inference_prefix"></a> [ml\_compute\_cluster\_inference\_prefix](#input\_ml\_compute\_cluster\_inference\_prefix) | The prefix name for this machine learning computer cluster (inference cluster). | `string` | `"clu-inference"` | no |
| <a name="input_ml_compute_cluster_inference_scale_down_nodes_after_idle_duration"></a> [ml\_compute\_cluster\_inference\_scale\_down\_nodes\_after\_idle\_duration](#input\_ml\_compute\_cluster\_inference\_scale\_down\_nodes\_after\_idle\_duration) | The node idle time before scale down: defines the time until the compute is shutdown when it has gone into idle state (inference cluster). | `string` | n/a | yes |
| <a name="input_ml_compute_cluster_inference_vm_priority"></a> [ml\_compute\_cluster\_inference\_vm\_priority](#input\_ml\_compute\_cluster\_inference\_vm\_priority) | The priority of the virtual machine (inference cluster). | `string` | n/a | yes |
| <a name="input_ml_compute_cluster_inference_vm_size"></a> [ml\_compute\_cluster\_inference\_vm\_size](#input\_ml\_compute\_cluster\_inference\_vm\_size) | The size of the virtual machine (inference cluster). | `string` | n/a | yes |
| <a name="input_ml_compute_cluster_training_max_node_count"></a> [ml\_compute\_cluster\_training\_max\_node\_count](#input\_ml\_compute\_cluster\_training\_max\_node\_count) | The maximum number of nodes to use on the cluster (training cluster). | `number` | n/a | yes |
| <a name="input_ml_compute_cluster_training_min_node_count"></a> [ml\_compute\_cluster\_training\_min\_node\_count](#input\_ml\_compute\_cluster\_training\_min\_node\_count) | The minimum number of nodes to use on the cluster (training cluster). | `number` | n/a | yes |
| <a name="input_ml_compute_cluster_training_prefix"></a> [ml\_compute\_cluster\_training\_prefix](#input\_ml\_compute\_cluster\_training\_prefix) | The prefix name for this machine learning computer cluster (training cluster). | `string` | `"clu-training"` | no |
| <a name="input_ml_compute_cluster_training_scale_down_nodes_after_idle_duration"></a> [ml\_compute\_cluster\_training\_scale\_down\_nodes\_after\_idle\_duration](#input\_ml\_compute\_cluster\_training\_scale\_down\_nodes\_after\_idle\_duration) | The node idle time before scale down: defines the time until the compute is shutdown when it has gone into idle state (training cluster). | `string` | n/a | yes |
| <a name="input_ml_compute_cluster_training_vm_priority"></a> [ml\_compute\_cluster\_training\_vm\_priority](#input\_ml\_compute\_cluster\_training\_vm\_priority) | The priority of the virtual machine (training cluster). | `string` | n/a | yes |
| <a name="input_ml_compute_cluster_training_vm_size"></a> [ml\_compute\_cluster\_training\_vm\_size](#input\_ml\_compute\_cluster\_training\_vm\_size) | The size of the virtual machine (training cluster). | `string` | n/a | yes |
| <a name="input_ml_datastore_name"></a> [ml\_datastore\_name](#input\_ml\_datastore\_name) | The name of the data store in ml workspace. | `string` | `"datablobstore"` | no |
| <a name="input_name"></a> [name](#input\_name) | The application name, used to name all resources. | `string` | n/a | yes |
| <a name="input_resource_group_name"></a> [resource\_group\_name](#input\_resource\_group\_name) | The resource group name. | `string` | n/a | yes |
| <a name="input_storage_account_replication_type"></a> [storage\_account\_replication\_type](#input\_storage\_account\_replication\_type) | The type of replication to use for this storage account (default). | `string` | n/a | yes |
| <a name="input_storage_account_tier"></a> [storage\_account\_tier](#input\_storage\_account\_tier) | The tier to use for this storage account (default). | `string` | n/a | yes |
| <a name="input_subnet_compute_id"></a> [subnet\_compute\_id](#input\_subnet\_compute\_id) | The subnet resource id (compute). | `string` | n/a | yes |
| <a name="input_uid"></a> [uid](#input\_uid) | The unique identifier, used to name all resources. | `string` | n/a | yes |
| <a name="input_workload"></a> [workload](#input\_workload) | The workload name, used to name resource group. | `string` | `"core"` | no |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_key_vault_name"></a> [key\_vault\_name](#output\_key\_vault\_name) | The name of the key vault. |
| <a name="output_machine_learning_workspace_name"></a> [machine\_learning\_workspace\_name](#output\_machine\_learning\_workspace\_name) | The name of the machine learning workspace. |
| <a name="output_ml_compute_cluster_data_preparation_name"></a> [ml\_compute\_cluster\_data\_preparation\_name](#output\_ml\_compute\_cluster\_data\_preparation\_name) | The name of the machine learning compute cluster (data preparation). |
| <a name="output_ml_compute_cluster_inference_name"></a> [ml\_compute\_cluster\_inference\_name](#output\_ml\_compute\_cluster\_inference\_name) | The name of the machine learning compute cluster (inference). |
| <a name="output_ml_compute_cluster_training_name"></a> [ml\_compute\_cluster\_training\_name](#output\_ml\_compute\_cluster\_training\_name) | The name of the machine learning compute cluster (training). |
| <a name="output_ml_compute_cluster_uami_client_id"></a> [ml\_compute\_cluster\_uami\_client\_id](#output\_ml\_compute\_cluster\_uami\_client\_id) | The client id associated with ml compute cluster uami. |
| <a name="output_ml_datastore_name"></a> [ml\_datastore\_name](#output\_ml\_datastore\_name) | The name of the data store in ml workspace. |
