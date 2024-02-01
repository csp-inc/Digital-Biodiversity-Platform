#!/bin/bash

set -e

export LOCATION=$1
export RESOURCE_GROUP_NAME=$2
export STORAGE_ACCOUNT_NAME=$3
export CONTAINER_NAME=$4

if [ $(az group exists --name $RESOURCE_GROUP_NAME) = false ]; then
  # Create resource group
  az group create \
    --location $LOCATION \
    --name $RESOURCE_GROUP_NAME
  echo "Resource group $STORAGE_ACCOUNT_NAME created."
fi

# Create storage account
az storage account create \
  --resource-group $RESOURCE_GROUP_NAME \
  --location $LOCATION \
  --name $STORAGE_ACCOUNT_NAME \
  --sku Standard_LRS \
  --encryption-services blob \
  --allow-blob-public-access false
echo "Storage account $STORAGE_ACCOUNT_NAME created."

# Create blob container
az storage container create \
  --name $CONTAINER_NAME \
  --account-name $STORAGE_ACCOUNT_NAME
echo "Blob container $STORAGE_ACCOUNT_NAME created."
