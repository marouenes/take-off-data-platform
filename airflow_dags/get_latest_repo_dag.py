"""
# Repo update DAG
The purpose of this system dag is to keep your repos that contain your
DAGs, pyspark flows, etc.. up to date.

## How to get your repos updated by this flow
Simply add the section get_latest_repos to your `config_prod.yml` or `config_dev.yml`.
section should contain two entries: `git_folder` which points to the git folder in
which DAGs are kept, and `repo_list` which is a list of repos

#### Example
    get_latest_repos:
        git_folder: /home/../git (s3 buckets?)
        repo_list:
        - repo_A
        - repo_B
        - repo_C
        - repo_D
				...
"""
from __future__ import print_function

import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime

import yaml
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


# Get environment variables from your Airflow environment.
MODE = os.environ.get('MODE', None)
USER = os.environ.get('USER', None)
BASE_PATH = os.environ['HOME']
PROFILE = BASE_PATH + '/git/scheduler'
config_yaml = os.path.join(PROFILE, 'profiles', 'config_dev.yaml')

# Define default DAG args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 1),
}

# Optional block
if MODE == 'prod':
    schedule = '25 */4 * * *'
else:
    schedule = None

# Read repo list from config
with open(config_yaml, 'r') as stream:
    profile_dict = yaml.safe_load(stream)

# Attempt reading the get_latest_repo entry from the configuration yaml
try:
    git_directory = Path(profile_dict['get_latest_repos']['git_folder'])
    repo_list = profile_dict['get_latest_repos']['repo_list']
    repo_paths = [git_directory / repo for repo in repo_list]
except KeyError:
    # In case of an error, dump the entire yaml in the exception message to
    # make it easier for the user to troubleshoot in the airflow error message
    yaml_str = yaml.dump(profile_dict, default_flow_style=False)
    message = f"Invalid 'get_latest_repo' entry in {config_yaml}: {yaml_str}"
    raise Exception(message)


def update_repo(repo_path: Path):
    """Attempt to run git pull on the repo in the input path.

    :param repo_path: The absolute path to the repository directory
    """

    # Make sure the repo directory exists
    if not repo_path.exists():
        print(f'The path {repo_path} does not exist: Clone the repo first!')
        sys.exit(1)

    # Run the git pull command
    print(f'Attempting to update {repo_path}')
    pull_cmd = f'cd {repo_path} && git pull origin main'
    process = subprocess.run(pull_cmd,
                             shell=True,
                             check=False,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)
    output = process.stdout.decode()
    print(f'The git pull process returned the following output:\n{output}')

    # Exit if the git pull command was successful
    if process.returncode == 0:
        print('The process exited successfully')
        return

    # If not successful, run git status command to help the user troubleshoot
    print(f'Error trying to update {repo_path}')
    status_cmd = f'cd {repo_path} && git status'
    process = subprocess.run(status_cmd,
                             shell=True,
                             check=False,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)
    output = process.stdout.decode()
    print(f'The git status process returned the following output:\n{output}')
    sys.exit(1)


dag = DAG(
    'get_latest_repo',
    tags=['scheduler', 'system_dag'], # Optional
    default_args=default_args,
    schedule_interval=None, # Optional
    doc_md=__doc__
)

for repo_path in repo_paths:
    update_task = PythonOperator(
        dag=dag,
        task_id=repo_path.name,
        python_callable=update_repo,
        op_args=[repo_path]
    )