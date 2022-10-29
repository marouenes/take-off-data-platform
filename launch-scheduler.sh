#!/bin/bash

# This script is used to launch the scheduler on a local machine.
# It is expected to be run on the local[*].

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

eval "$(conda shell.bash hook)" && conda activate airflow-dev

# {{{ parse a yaml file
# shellcheck source=parse_yml.sh
# source parse_yml.sh
# # }}}

# # set an option for the cli to read the scheduler profile config from profiles/dev/user.yml
# while getopts ":p:" opt; do
#     case $opt in
#         p) profile="$OPTARG"
#         ;;
#         \?) echo "Invalid option -$OPTARG" >&2
#         ;;
#     esac
# done

# # parse the profile config

# # shellcheck disable=SC1090
# source profiles/"$profile"/user.yml

# The scheduler will be launched in the background, and its process ID
# will be written to the file specified by the PID_FILE variable.
export PID_FILE=/tmp/scheduler.pid
info "Setting the process ID for the scheduler to $PID_FILE"

# TODO: check if the airflow home is set, and if not, set it to the default
export AIRFLOW_HOME=~/airflow
info "Setting the AIRFLOW_HOME environment variable to $AIRFLOW_HOME"

# set a run mode for a local scheduler
export MODE="debug"

# set the user to run the scheduler as
USER="$(whoami)"
export USER

# set the scheduler dag directory owner
export AIRFLOW_CTX_DAG_OWNER=$USER

# add the ssh key to the ssh agent
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_rsa_thermofisher
info "Adding the ssh key to the ssh agent"

# info "Setting the AIRFLOW__CORE__EXECUTOR environment variable to LocalExecutor"
# TODO: issue with sqlite when running with LocalExecutor
# export AIRFLOW__CORE__EXECUTOR=LocalExecutor

info "Setting a fernet key for airflow"
: "${AIRFLOW__CORE__FERNET_KEY:=${FERNET_KEY:=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")}}"

echo \
"====================================================
ENVIROMENT VARIABLES SET FOR AIRFLOW:
AIRFLOW_CTX_DAG_OWNER=$AIRFLOW_CTX_DAG_OWNER
AIRFLOW_HOME=$AIRFLOW_HOME
MODE=$MODE
USER=$USER
===================================================="

info "Cheking if the airflow home directory exists"
if [ -d "$AIRFLOW_HOME" ]; then
    success "Airflow home directory exists."
else
    warn "Airflow home directory does not exist. Creating it now."
    airflow db init
fi

airflow db upgrade

cd ~/airflow || exit

# decalre -a args

# args=("--daemon" "--pid" "$PID_FILE" "--stdout" "$AIRFLOW_HOME/logs/scheduler.log" "--stderr" "$AIRFLOW_HOME/logs/scheduler.log")

# airflow scheduler "${args[@]}"

# set the credentials for the user
username="admin"
password="admin"
firstname="foo"
lastname="bar"
email="foo.bar@foo.bar"

# create an admin user
airflow users create \
    --username $username \
    --password $password \
    --firstname $firstname \
    --lastname $lastname \
    --role Admin \
    --email $email

# start the scheduler
info "Starting the scheduler"
airflow scheduler -D &>/dev/null

info "scheduler running PID at $PID_FILE"
echo $! > $PID_FILE

# # wait for the scheduler to finish
# wait "$(cat $PID_FILE)"

success "scheduler setup finished"
info "starting the webserver on port 8080"
airflow webserver -p 8080
