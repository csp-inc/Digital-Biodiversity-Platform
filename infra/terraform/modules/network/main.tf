# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

terraform {
  required_version = ">=1.3.6"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "=3.82.0"
    }
  }
}

locals {
  resource_version         = "${var.name}-${var.environment}-${var.uid}"
  resource_version_compact = "${var.name}${var.environment}${var.uid}"
  required_tags = {
    name        = var.name
    environment = var.environment
    uid         = var.uid
    workload    = var.workload
  }
}

# create resource group
resource "azurerm_resource_group" "default" {
  name     = var.resource_group_name
  location = var.location
  tags     = local.required_tags
}

#--------------------------------------------------------------
# Virtual network and Subnets
#--------------------------------------------------------------

# create virtual network
resource "azurerm_virtual_network" "default" {
  name                = "vnet-${local.resource_version}"
  location            = azurerm_resource_group.default.location
  resource_group_name = azurerm_resource_group.default.name
  address_space       = var.virtual_network_address_space
  tags                = local.required_tags
}

# create subnet (compute)
resource "azurerm_subnet" "compute" {
  name                 = "snet-compute-${local.resource_version}"
  resource_group_name  = azurerm_resource_group.default.name
  virtual_network_name = azurerm_virtual_network.default.name
  address_prefixes     = var.compute_subnet_address_space
}
