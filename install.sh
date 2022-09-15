#!/bin/bash

# This script is used to install airflow locally.

# Init option {{{
Color_off='\033[0m'       # Text Reset

# terminal color template {{{
# Regular Colors
Red='\033[0;31m'          # Red
Green='\033[0;32m'        # Green
Yellow='\033[0;33m'       # Yellow
Blue='\033[0;34m'         # Blue

# success/info/error/warn {{{
msg() {
    printf '%b\n' "$1" >&2
}

success() {
    msg "${Green}[✔]${Color_off} ${1}${2}"
}

info() {
    msg "${Blue}[➭]${Color_off} ${1}${2}"
}

error() {
    msg "${Red}[✘]${Color_off} ${1}${2}"
    exit 1
}

warn () {
    msg "${Yellow}[⚠]${Color_off} ${1}${2}"
}
# }}}

# echo_with_color {{{
echo_with_color () {
    printf '%b\n' "$1$2$Color_off" >&2
}
# }}}

# # create a conda environment {{{
# info "Creating a conda environment for airflow"
# conda create -n airflow python=3.7 -y
# # }}}

# # refresh the shell {{{
# info "Refreshing the shell"
# source ~/.bashrc
# # }}}

# # activate the conda environment {{{
# info "Activating the conda environment for airflow"
# conda activate airflow
# # }}}

# installing pyspark {{{
info "Installing pyspark and some other dev packages"
pip install -r requirements-dev.txt
# }}}

# Install Airflow using the constraints file
info "Installing airflow using the constraints file"
info "This will take a while..."
info "Airflow will be installed in the current python environment (airflow)"

AIRFLOW_VERSION=2.2.3
info "Installing airflow version $AIRFLOW_VERSION"
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
info "Installing airflow for python version $PYTHON_VERSION"

# For example: 3.7
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example: https://raw.githubusercontent.com/apache/airflow/constraints-2.3.4/constraints-3.7.txt

info "Installing airflow using the constraints file $CONSTRAINT_URL"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

success "Airflow installed successfully"
