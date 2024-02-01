#!/bin/bash
# shellcheck source-path=SCRIPTDIR/../scripts
#
# executable
#
# install tools in the development environment

set -e

function installTerraformDocs() {
    pushd /tmp > /dev/null
    envArchitecture=$(arch)

    if [ "${envArchitecture}" = "aarch64" ]; then
        envArchitecture="arm64"
    fi

    if [ "${envArchitecture}" = "x86_64" ]; then
        envArchitecture="amd64"
    fi

    # Install TerraformDocs
    curl -sSLo ./terraform-docs.tar.gz https://terraform-docs.io/dl/v0.16.0/terraform-docs-v0.16.0-linux-${envArchitecture}.tar.gz
    tar -xzf terraform-docs.tar.gz
    chmod +x terraform-docs
    mv terraform-docs /usr/local/bin/terraform-docs

    popd > /dev/null
}

installTerraformDocs
