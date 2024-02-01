# Network module (terraform-docs)

## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_terraform"></a> [terraform](#requirement\_terraform) | >=1.3.6 |
| <a name="requirement_azurerm"></a> [azurerm](#requirement\_azurerm) | =3.82.0 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_azurerm"></a> [azurerm](#provider\_azurerm) | =3.82.0 |

## Modules

No modules.

## Resources

| Name | Type |
|------|------|
| [azurerm_resource_group.default](https://registry.terraform.io/providers/hashicorp/azurerm/3.82.0/docs/resources/resource_group) | resource |
| [azurerm_subnet.compute](https://registry.terraform.io/providers/hashicorp/azurerm/3.82.0/docs/resources/subnet) | resource |
| [azurerm_virtual_network.default](https://registry.terraform.io/providers/hashicorp/azurerm/3.82.0/docs/resources/virtual_network) | resource |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_compute_subnet_address_space"></a> [compute\_subnet\_address\_space](#input\_compute\_subnet\_address\_space) | The subnet address space (compute). | `list(string)` | n/a | yes |
| <a name="input_environment"></a> [environment](#input\_environment) | The environment name, used to name all resources. | `string` | n/a | yes |
| <a name="input_location"></a> [location](#input\_location) | The azure region targeted. | `string` | n/a | yes |
| <a name="input_name"></a> [name](#input\_name) | The application name, used to name all resources. | `string` | n/a | yes |
| <a name="input_resource_group_name"></a> [resource\_group\_name](#input\_resource\_group\_name) | The resource group name. | `string` | n/a | yes |
| <a name="input_uid"></a> [uid](#input\_uid) | The unique identifier, used to name all resources. | `string` | n/a | yes |
| <a name="input_virtual_network_address_space"></a> [virtual\_network\_address\_space](#input\_virtual\_network\_address\_space) | The virtual network address space (compute). | `list(string)` | n/a | yes |
| <a name="input_workload"></a> [workload](#input\_workload) | The workload name, used to name resource group. | `string` | `"net"` | no |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_subnet_compute_id"></a> [subnet\_compute\_id](#output\_subnet\_compute\_id) | The subnet resource id (compute). |
