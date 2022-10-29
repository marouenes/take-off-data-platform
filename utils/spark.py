"""
Module for SparkSubmitOperator with zip functionality
"""

# pylint: disable=C0103 # Disable warning about uppercase var names
# pylint: disable=W0104 # Disable warning about non-effect statements on tasks

import os
import pathlib
import subprocess
import time
import zipfile
from datetime import datetime

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models.connection import Connection


def zip_directory(path: str, output: str, include_symlinks: bool):
    """
    Zip all python files in a directory
    :param path: The path to the directory
    :param output: The path to the output zip file
    :param include_symlinks: If symlink dirs. should be included in the zip
    """

    with zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for root, dirs, files in os.walk(path, followlinks=include_symlinks):
            # Exclude hidden dot-directories like .pip
            dirs[:] = [d for d in dirs if not d.startswith('.')]
            # Only select python files with the extension .py
            files = [f for f in files if f.endswith('.py')]
            for file in files:
                rel_path = os.path.relpath(os.path.join(root, file), path)
                zip_file.write(os.path.join(root, file), rel_path)


class SparkOperator(SparkSubmitOperator):
    """
    Utility wrapper class with zip functionality and log collection capabilities
    in spark cluster mode for a SparkSubmitOperator.
    """

    # pylint: disable=too-few-public-methods

    def __init__(self, repo_path: str = None, include_symlinks: bool = False, **kwargs):
        """
        Utility wrapper for a SparkSubmitOperator that will also zip and
        include code for an entire repo when executing Spark jobs.
        :param repo_path: The path to the repository containing the code
        :param include_symlinks: If symlink dirs. should be included in the zip
        :param kwargs: Additional arguments passed to SparkSubmitOperator
        """

        super().__init__(**kwargs)
        self.repo_path = repo_path
        self.include_symlinks = include_symlinks
        self.task_exited = False

        if repo_path is not None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
            self.zip_path = f'/tmp/{self.task_id}_{timestamp}.zip'
        else:
            self.zip_path = None

        # We need to create the zip file path dynamically. Since this can't be
        # done before the super call we can't assign it as a keyword parameter
        # and have to assign it after. It is not used in any special way in the
        # constructor though, so this is safe.
        self._py_files = self.zip_path

        spark_conn = Connection.get_connection_from_secrets(kwargs['conn_id'])
        self.deploy_mode = spark_conn.extra_dejson['deploy-mode']

        # https://medium.com/@cupreous.bowels/logging-in-spark-with-log4j-how-to-customize-a-driver-and-executors-for-yarn-cluster-mode-1be00b984a7c
        if self.deploy_mode == 'cluster':
            file_path = pathlib.Path(__file__).parent.resolve()
            driver_properties = f'{file_path}/log4j/log4j-driver.properties'
            executor_properties = f'{file_path}/log4j/log4j-executor.properties'
            if self._conf is None:
                self._conf = {}
            self._conf['spark.driver.extraJavaOptions'] = (
                '"-Dlog4j.configuration=file:log4j-driver.properties"'
            )
            self._conf['spark.executor.extraJavaOptions'] = (
                '"-Dlog4j.configuration=file:log4j-executor.properties"'
            )
            self._files = f'{driver_properties},{executor_properties}'

    def read_log(self):
        """
        Read the YARN application logs and print to the airflow log.
        """
        yarn_application_id = self._hook._yarn_application_id

        if yarn_application_id is None:
            print('Skipping logs: Could not find the YARN application ID.')
            return

        print(f'Reading log contents for {yarn_application_id}')
        command = ['yarn', 'logs', '-applicationId', yarn_application_id]
        proc = subprocess.Popen(command, stdout=subprocess.PIPE)

        print('Found the following log contents from the application')
        for line in proc.stdout.readlines():
            print(line.decode('utf-8').rstrip())

    def task_exit(self):
        """
        Function called when the task exits.
        """
        # For safety, make sure we have not called this function before
        if self.task_exited:
            return

        # Remove the temporary zip file
        if self.zip_path is not None and os.path.exists(self.zip_path):
            os.remove(self.zip_path)
            print(f'File removed: {self.zip_path}')

        # If we are in cluster mode, read the log contents
        if self.deploy_mode == 'cluster':
            # Wait to let the YARN have time to write logs before reading
            # TODO: Waiting for a set time is not very robust. Better options?
            time.sleep(1)
            self.read_log()

        self.task_exited = True

    def task_entry(self):
        """
        Function called when the task starts.
        """
        if self.zip_path is not None:
            print(f'Zipping {self.repo_path} to {self.zip_path}')
            zip_directory(
                path=self.repo_path,
                output=self.zip_path,
                include_symlinks=self.include_symlinks,
            )
            print(f'Running SparkSubmitOperator with py_files={self.zip_path}')

    def pre_execute(self, context: dict):
        """
        Create the temporary zip file before executing the task.
        """
        self.task_entry()
        super().pre_execute(context=context)

    def execute(self, context: dict):
        """
        Execute the task, and catch exceptions to trigger the custom task_exit
        before the application exits completely. Then continue raising the
        exception after task_exit has completed
        """
        try:
            super().execute(context=context)
        except Exception:
            self.task_exit()
            raise

    def post_execute(self, context: dict, result: dict = None):
        """
        Perform the post_execute and call the task_exit function.
        """
        super().post_execute(context=context, result=result)
        self.task_exit()

    def on_kill(self):
        """
        Perform the on_kill and call the task_exit function.
        """
        super().on_kill()
        self.task_exit()
