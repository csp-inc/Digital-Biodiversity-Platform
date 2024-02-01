# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

output "subnet_compute_id" {
  description = "The subnet resource id (compute)."
  value       = azurerm_subnet.compute.id
}
