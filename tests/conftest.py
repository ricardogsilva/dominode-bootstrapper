import json
import logging
import shlex
import subprocess
import urllib3
from configparser import ConfigParser
from time import sleep
from urllib3.exceptions import MaxRetryError

import docker
import pytest
import sqlalchemy as sla
from minio import Minio
from sqlalchemy.exc import OperationalError

logger = logging.getLogger(__name__)


DB_SERVICE_NAME = 'dominode-db-dev'


@pytest.fixture(scope='session')
def db_users_credentials():
    return {
        'ppd_editor1': ('ppd_editor1', 'ppd', 'editor'),
        'ppd_editor2': ('ppd_editor2', 'ppd', 'editor'),
        'ppd_user1': ('ppd_user1', 'ppd', 'regular_department_user'),
        'ppd_user2': ('ppd_user2', 'ppd', 'regular_department_user'),
        'lsd_editor1': ('lsd_editor1', 'lsd', 'editor'),
        'lsd_editor2': ('lsd_editor2', 'lsd', 'editor'),
        'lsd_user1': ('lsd_user1', 'lsd', 'regular_department_user'),
        'lsd_user2': ('lsd_user2', 'lsd', 'regular_department_user'),
    }


@pytest.fixture(scope='session')
def db_admin_credentials():
    return {
        'host': 'localhost',
        'db': 'dominode_pytest',
        'port': '55432',
        'user': 'dominode_test',
        'password': 'dominode_test',
    }


@pytest.fixture(scope='session')
def docker_client():
    return docker.from_env()


@pytest.fixture(scope='session')
def db_container(docker_client, db_admin_credentials):
    container = docker_client.containers.run(
        'postgis/postgis:12-3.0-alpine',
        detach=True,
        name=db_admin_credentials['db'],
        remove=True,
        ports={
            '5432': db_admin_credentials['port']
        },
        environment={
            'POSTGRES_DB': db_admin_credentials['db'],
            'POSTGRES_USER': db_admin_credentials['user'],
            'POSTGRES_PASSWORD': db_admin_credentials['password'],
        }

    )
    yield container
    logger.info(f'Removing container...')
    container.stop()


@pytest.fixture(scope='session')
def db_connection(db_container, db_admin_credentials):
    engine = sla.create_engine(
        'postgresql://{user}:{password}@{host}:{port}/{db}'.format(
            user=db_admin_credentials['user'],
            password=db_admin_credentials['password'],
            host=db_admin_credentials['host'],
            port=db_admin_credentials['port'],
            db=db_admin_credentials['db']
        )
    )
    connected = False
    max_tries = 30
    current_try = 0
    sleep_for = 2  # seconds
    while not connected and current_try < max_tries:
        try:
            with engine.connect() as connection:
                connected = True
                yield connection
                logger.info('Closing DB connection...')
        except OperationalError:
            print(f'Could not connect to DB ({current_try + 1}/{max_tries})')
            current_try += 1
            if current_try < max_tries:
                sleep(sleep_for)
            else:
                raise


@pytest.fixture(scope='session')
def bootstrapped_db_connection(db_connection, db_admin_credentials):
    completed_process = subprocess.run(
        shlex.split(
            f'dominode-admin db bootstrap '
            f'--db-admin-username={db_admin_credentials["user"]} '
            f'--db-admin-password={db_admin_credentials["password"]} '
            f'--db-name={db_admin_credentials["db"]} '
            f'--db-host={db_admin_credentials["host"]} '
            f'--db-port={db_admin_credentials["port"]}'
        ),
        capture_output=True
    )
    try:
        completed_process.check_returncode()
    except subprocess.CalledProcessError:
        print(f'stdout: {completed_process.stdout}')
        print(f'stderr: {completed_process.stderr}')
        raise


@pytest.fixture(scope='session')
def db_users(
        bootstrapped_db_connection,
        db_users_credentials,
        db_admin_credentials
):
    for user, user_info in db_users_credentials.items():
        password, department, role = user_info
        completed_process = subprocess.run(
            shlex.split(
                f'dominode-admin db add-department-user '
                f'{user} '
                f'{password} '
                f'{department} '
                f'--role={role} '
                f'--db-admin-username={db_admin_credentials["user"]} '
                f'--db-admin-password={db_admin_credentials["password"]} '
                f'--db-host={db_admin_credentials["host"]} '
                f'--db-port={db_admin_credentials["port"]} '
                f'--db-name={db_admin_credentials["db"]}'
            ),
            capture_output=True
        )
        try:
            completed_process.check_returncode()
        except subprocess.CalledProcessError:
            print(f'stdout: {completed_process.stdout}')
            print(f'stderr: {completed_process.stderr}')
            raise


@pytest.fixture(scope='session')
def minio_server_info():
    return {
        'port': 9200,
        'access_key': 'myuser',
        'secret_key': 'mypassword',
    }


@pytest.fixture(scope='session')
def minio_server(docker_client, minio_server_info):
    container = docker_client.containers.run(
        'minio/minio',
        detach=True,
        command='server /data',
        name='minio-server-pytest',
        auto_remove=True,
        ports={
            9000: minio_server_info['port']
        },
        environment={
            'MINIO_ACCESS_KEY': minio_server_info['access_key'],
            'MINIO_SECRET_KEY': minio_server_info['secret_key'],
        }
    )
    yield container
    logger.info(f'Removing container...')
    container.stop()


@pytest.fixture(scope='session')
def minio_client(minio_server, minio_server_info):
    max_tries = 10
    sleep_for = 2  # seconds

    for current_try in range(1, max_tries + 1):
        print(f'current_try: {current_try}')
        try:
            endpoint = f'localhost:{minio_server_info["port"]}'
            print(f'endpoint: {endpoint}')
            print(f'access_key: {minio_server_info["access_key"]}')
            print(f'secret_key: {minio_server_info["secret_key"]}')
            client = Minio(
                endpoint=endpoint,
                access_key=minio_server_info['access_key'],
                secret_key=minio_server_info['secret_key'],
                secure=False,
                # customizing http_client because we don't want the default
                # timeout configuration applied
                http_client=urllib3.PoolManager(
                    timeout=urllib3.Timeout.DEFAULT_TIMEOUT,
                    maxsize=10,
                    retries=None
                )
            )
            print(f'client: {client}')
            # perform some command to see if we can connect
            result = client.list_buckets()
            print(f'result: {result}')
            break
        except MaxRetryError:
            print(
                f'Could not connect to minIO server '
                f'({current_try}/{max_tries})'
            )
            if current_try < max_tries:
                sleep(sleep_for)
        except Exception:
            raise
    else:
        raise RuntimeError(f'Gave up on minIO server')
    return client


@pytest.fixture()
def minio_admin_client(minio_server, minio_server_info):
    endpoint = f'localhost:{minio_server_info["port"]}'
    client = Minio(
        endpoint=endpoint,
        access_key=minio_server_info['access_key'],
        secret_key=minio_server_info['secret_key'],
        secure=False,
    )
    return client


@pytest.fixture(scope='session')
def minio_users_credentials():
    return {
        'ppd_editor1': ('ppd_editor1', 'ppd', 'editor'),
        'ppd_editor2': ('ppd_editor2', 'ppd', 'editor'),
        'ppd_user1': ('ppd_user1', 'ppd', 'regular_department_user'),
        'ppd_user2': ('ppd_user2', 'ppd', 'regular_department_user'),
        'lsd_editor1': ('lsd_editor1', 'lsd', 'editor'),
        'lsd_editor2': ('lsd_editor2', 'lsd', 'editor'),
        'lsd_user1': ('lsd_user1', 'lsd', 'regular_department_user'),
        'lsd_user2': ('lsd_user2', 'lsd', 'regular_department_user'),
    }


@pytest.fixture(scope='session')
def bootstrapped_minio_server(
        minio_server,
        minio_server_info,
        minio_client,  # here just to make sure server is already usable
        minio_users_credentials,
):
    server_alias = 'dominode-pytest'
    command_kwargs = (
        f'--access-key={minio_server_info["access_key"]} '
        f'--secret-key={minio_server_info["secret_key"]} '
        f'--alias={server_alias} '
        f'--host=localhost '
        f'--port={minio_server_info["port"]} '
        f'--protocol=http '
    )
    completed_process = subprocess.run(
        shlex.split(
            f'dominode-admin minio bootstrap {command_kwargs} '
        ),
        capture_output=True
    )
    try:
        completed_process.check_returncode()
    except subprocess.CalledProcessError:
        print(f'stdout: {completed_process.stdout}')
        print(f'stderr: {completed_process.stderr}')
        raise

    for access_key, user_info in minio_users_credentials.items():
        secret_key, department_name, role = user_info
        completed_process = subprocess.run(
            shlex.split(
                f'dominode-admin minio add-department-user {command_kwargs} '
                f'--role {role} {access_key} {secret_key} {department_name} '
            ),
            capture_output=True
        )
        try:
            completed_process.check_returncode()
        except subprocess.CalledProcessError:
            print(f'stdout: {completed_process.stdout}')
            print(f'stderr: {completed_process.stderr}')
            raise
