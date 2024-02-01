# Configuration files using Terraform

## Infrastucture as Code

We manage our infrastructure through configuration files and templates throughout their lifecycle, the same approach we take to code.

We used [Terraform](https://www.terraform.io/) to automate the provisioning of the infrastructure.

## Terraform

The following [modules](https://developer.hashicorp.com/terraform/language/modules) are written using [HCL configuration syntax](https://developer.hashicorp.com/terraform/language/syntax/configuration) to provision resources.

- [root module](../../../infra/terraform/main.tf)
  - Main entry point, purpose of this module is to call childs modules (network, core).
- [network module](../../../infra/terraform/modules/network/main.tf)
  - Reference existing virtual network in terraform state, and create subnets (workspace, compute, inference).
- [core module](../../../infra/terraform/modules/core/main.tf)
  - Create all additional resources listed in [infrastructure](./infrastructure.md) documentation and [core module readme](../../../infra/terraform/modules/core/README.md).

Module are containers for multiple resources that are used together. It's a way to split the code for making it composable and reusable ; or just offer better readability.

All modules contains the same file structure:

- variables.tf
  - [Inputs variables](https://developer.hashicorp.com/terraform/language/values/variables) needed by the module.
- outputs.tf
  - [Outputs values](https://developer.hashicorp.com/terraform/language/values/outputs) produced by the module.
- main.tf
  - [Resources](https://developer.hashicorp.com/terraform/language/resources) created by the modules.
- data.tf
  - [Data sources](https://developer.hashicorp.com/terraform/language/data-sources) referenced by the module (it's usually Azure resource defined outside of Terraform).

## Variables

A list of variables is defined and associated values are required to deploy a specific environment (`Development`).

- [variables.tf](../../../infra/terraform/variables.tf)
  - Declaration of all variables names and types.

When terraform applies configuration files, we can set values with usage of .tfvars file.

- [development.tfvars](../../../infra/terraform/environments/development.tfvars)

Thanks to these variables, you can update all SKUs on deployed resources if you want to scale up/down the solution.
