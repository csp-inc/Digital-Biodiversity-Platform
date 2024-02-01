# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

ARG VARIANT="3.8"
FROM --platform=linux/amd64 mcr.microsoft.com/vscode/devcontainers/python:0-${VARIANT}

# [Choice] Node.js version: none, lts/*, 16, 14, 12, 10
ARG NODE_VERSION="16"
RUN if [ "${NODE_VERSION}" != "none" ]; then su vscode -c "umask 0002 && . /usr/local/share/nvm/nvm.sh && nvm install ${NODE_VERSION} 2>&1"; fi

# Install some basics packages
RUN apt-get update && \
  apt-get upgrade -y && \
  apt-get install -y sudo git curl wget make procps unzip pandoc jq black dos2unix

ARG USERNAME=vscode

# Python things: update pip, install az-cli
RUN python3 -m pip install pip --upgrade

COPY .devcontainer/requirements.txt /home/$USERNAME/
RUN pip --disable-pip-version-check --no-cache-dir install -r /home/$USERNAME/requirements.txt

# Install missing packages out of apt-get
COPY /.devcontainer/tools-install.sh /home/$USERNAME/tools-install.sh
RUN chmod +x /home/$USERNAME/tools-install.sh
RUN dos2unix /home/$USERNAME/tools-install.sh && /home/$USERNAME/tools-install.sh

USER $USERNAME

# update npm
RUN npm install -g npm@latest

# typescript
RUN npm install -g tslint-to-eslint-config typescript markdown-link-check markdownlint-cli && npm cache clean --force > /dev/null 2>&1


