# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

variable "name" {
  description = "The application name, used to name all resources."
  type        = string
}

variable "workload" {
  description = "The workload name, used to name resource group."
  type        = string
  default     = "net"
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

variable "virtual_network_address_space" {
  description = "The virtual network address space (compute)."
  type        = list(string)
}

variable "compute_subnet_address_space" {
  description = "The subnet address space (compute)."
  type        = list(string)
}
