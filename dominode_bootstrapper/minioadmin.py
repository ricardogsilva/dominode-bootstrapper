"""Extra admin commands to manage the DomiNode minIO server

This script adds some functions to perform DomiNode related tasks in a more
expedite manner than using the bare minio client `mc`.

"""

import json
import os
import shlex
import subprocess
import tempfile
import typing
from contextlib import contextmanager
from pathlib import Path

import typer

from .constants import UserRole
from . import utils

_help_intro = 'Manage minIO server'

app = typer.Typer(
    short_help=_help_intro,
    help=(
        f'{_help_intro} - Be sure to install minio CLI client (mc) before '
        f'using this. Also, create a \'~/.mc/config.json\' file with the '
        f'credentials of the minIO server that you want to use. Check out the'
        f'minIO client docs at: \n\n'
        f'https://docs.min.io/docs/minio-client-quickstart-guide.html\n\n'
        f'for details on how to download mc and configure it.'
    )
)

SUCCESS = "success"
DEFAULT_CONFIG_DIR = Path('~/.mc').expanduser()
config = utils.load_config()


class DomiNodeDepartment:
    name: str
    minio_parameters: typing.Dict
    dominode_staging_bucket: str = 'dominode-staging'
    public_bucket: str = 'public'
    _policy_version: str = '2012-10-17'

    def __init__(
            self,
            name: str,
            minio_endpoint_alias: str,
            minio_access_key: str,
            minio_secret_key: str,
            minio_host: str,
            minio_port: int = 9000,
            minio_protocol: str = 'https',
    ):
        self.name = name
        self.minio_parameters = {
            'alias': minio_endpoint_alias,
            'access_key': minio_access_key,
            'secret_key': minio_secret_key,
            'host': minio_host,
            'port': minio_port,
            'protocol': minio_protocol
        }

    @property
    def staging_bucket(self) -> str:
        return f'{self.name}-staging'

    @property
    def dominode_staging_root_dir(self) -> str:
        return f'{self.dominode_staging_bucket}/{self.name}/'

    @property
    def production_bucket_root_dir(self) -> str:
        return f'{self.public_bucket}/{self.name}/'

    @property
    def regular_users_group(self) -> str:
        return f'{self.name}-user'

    @property
    def editors_group(self) -> str:
        return f'{self.name}-editor'

    @property
    def regular_user_policy(self) -> typing.Tuple[str, typing.Dict]:
        return (
            f'{self.name}-regular-user-group-policy',
            {
                'Version': self._policy_version,
                'Statement': [
                    {
                        'Sid': f'{self.name}-regular-user-deny-bucket-delete',
                        'Action': [
                            's3:DeleteBucket',
                        ],
                        'Effect': 'Deny',
                        'Resource': [
                            f'arn:aws:s3:::{self.dominode_staging_bucket}',
                            f'arn:aws:s3:::{self.staging_bucket}',
                        ]
                    },
                    {
                        'Sid': f'{self.name}-regular-user-full-access',
                        'Action': [
                            's3:*'
                        ],
                        'Effect': 'Allow',
                        'Resource': [
                            f'arn:aws:s3:::{self.dominode_staging_root_dir}*',
                            f'arn:aws:s3:::{self.staging_bucket}/*',
                        ]
                    },
                    {
                        'Sid': f'{self.name}-regular-user-read-only',
                        'Action': [
                            's3:GetBucketLocation',
                            's3:ListBucket',
                            's3:GetObject',
                        ],
                        'Effect': 'Allow',
                        'Resource': [
                            f'arn:aws:s3:::{self.dominode_staging_bucket}/*',
                            f'arn:aws:s3:::{self.public_bucket}/*'
                        ]
                    },
                ]
            }
        )

    @property
    def editor_user_policy(self) -> typing.Tuple[str, typing.Dict]:
        return (
            f'{self.name}-editor-group-policy',
            {
                'Version': self._policy_version,
                'Statement': [
                    {
                        'Sid': f'{self.name}-editor-user-deny-bucket-delete',
                        'Action': [
                            's3:DeleteBucket',
                        ],
                        'Effect': 'Deny',
                        'Resource': [
                            f'arn:aws:s3:::{self.dominode_staging_bucket}',
                            f'arn:aws:s3:::{self.staging_bucket}',
                        ]
                    },
                    {
                        'Sid': f'{self.name}-editor-full-access',
                        'Action': [
                            's3:*'
                        ],
                        'Effect': 'Allow',
                        'Resource': [
                            f'arn:aws:s3:::{self.staging_bucket}/*',
                            f'arn:aws:s3:::{self.dominode_staging_root_dir}*',
                            f'arn:aws:s3:::{self.production_bucket_root_dir}*',
                        ]
                    },
                    {
                        'Sid': f'{self.name}-editor-read-only',
                        'Action': [
                            's3:GetBucketLocation',
                            's3:ListBucket',
                            's3:GetObject',
                        ],
                        'Effect': 'Allow',
                        'Resource': [
                            f'arn:aws:s3:::{self.dominode_staging_bucket}/*',
                            f'arn:aws:s3:::{self.public_bucket}/*'
                        ]
                    },
                ]
            }
        )

    def create_groups(self):
        create_group(self.regular_users_group, **self.minio_parameters)
        create_group(self.editors_group, **self.minio_parameters)

    def create_buckets(self):
        extra = '--ignore-existing'
        self._execute_command('mb', f'{self.staging_bucket} {extra}')
        self._execute_command('mb', f'{self.dominode_staging_root_dir} {extra}')
        self._execute_command(
            'mb', f'{self.production_bucket_root_dir} {extra}')

    def create_policies(self):
        self.add_policy(*self.regular_user_policy)
        self.add_policy(*self.editor_user_policy)

    def add_policy(self, name: str, policy: typing.Dict):
        """Add policy to the server"""
        existing_policies = self._execute_admin_command('policy list')
        for item in existing_policies:
            if item.get('policy') == name:
                break  # policy already exists
        else:
            os_file_handler, pathname = tempfile.mkstemp(text=True)
            with os.fdopen(os_file_handler, mode='w') as fh:
                json.dump(policy, fh)
            self._execute_admin_command(
                'policy add',
                f'{name} {pathname}',
            )
            Path(pathname).unlink(missing_ok=True)

    def set_policies(self):
        self.set_policy(self.regular_user_policy[0], self.regular_users_group)
        self.set_policy(self.editor_user_policy[0], self.editors_group)
        self._set_public_policy()

    def _set_public_policy(self):
        self._execute_command(
            'policy set download',
            f'{self.production_bucket_root_dir}*'
        )

    def set_policy(
            self,
            policy: str,
            group: str,
    ):
        self._execute_admin_command(
            'policy set',
            f'{policy} group={group}',
        )

    def add_user(
            self,
            access_key: str,
            secret_key: str,
            role: typing.Optional[UserRole] = UserRole.REGULAR_DEPARTMENT_USER
    ):
        create_user(access_key, secret_key, **self.minio_parameters)
        group = {
            UserRole.REGULAR_DEPARTMENT_USER: self.regular_users_group,
            UserRole.EDITOR: self.editors_group,
        }[role]
        addition_result = self._execute_admin_command(
            'group add', f'{group} {access_key}',)
        return addition_result[0].get('status') == SUCCESS

    def _execute_command(
            self,
            command: str,
            arguments: typing.Optional[str] = None,
    ):
        return execute_command(
            command,
            **self.minio_parameters,
            arguments=arguments,
        )

    def _execute_admin_command(
            self,
            command: str,
            arguments: typing.Optional[str] = None,
    ):
        return execute_minio_admin_command(
            command,
            **self.minio_parameters,
            arguments=arguments,
        )


@app.command()
def add_department_user(
        user_access_key: str,
        user_secret_key: str,
        department_name: str,
        role: typing.Optional[UserRole] = UserRole.REGULAR_DEPARTMENT_USER,
        access_key: typing.Optional[str] = config['minio']['admin_access_key'],
        secret_key: typing.Optional[str] = config['minio']['admin_secret_key'],
        alias: str = 'dominode_bootstrapper',
        host: typing.Optional[str] = config['minio']['host'],
        port: typing.Optional[int] = config['minio']['port'],
        protocol: typing.Optional[str] = config['minio']['protocol']

):
    """Create a user and add it to the relevant department groups

    This function shall ensure that when a new user is created it is put in the
    relevant groups and with the correct access policies

    """

    department = DomiNodeDepartment(
        department_name, alias, access_key, secret_key, host,
        port, protocol
    )
    return department.add_user(user_access_key, user_secret_key, role)


@app.command()
def add_department(
        name: str,
        access_key: typing.Optional[str] = config['minio']['admin_access_key'],
        secret_key: typing.Optional[str] = config['minio']['admin_secret_key'],
        alias: str = 'dominode_bootstrapper',
        host: typing.Optional[str] = config['minio']['host'],
        port: typing.Optional[int] = config['minio']['port'],
        protocol: typing.Optional[str] = config['minio']['protocol']
):
    """Add a new department

    This includes:

    -  Adding department staging bucket
    -  Adding department groups

    """

    department = DomiNodeDepartment(
        name, alias, access_key, secret_key, host, port, protocol)
    typer.echo(f'Creating groups...')
    department.create_groups()
    typer.echo(f'Creating buckets...')
    department.create_buckets()
    typer.echo(f'Creating policies...')
    department.create_policies()
    typer.echo(f'Setting policies...')
    department.set_policies()


@app.command()
def bootstrap(
        access_key: typing.Optional[str] = config['minio']['admin_access_key'],
        secret_key: typing.Optional[str] = config['minio']['admin_secret_key'],
        alias: str = 'dominode_bootstrapper',
        host: typing.Optional[str] = config['minio']['host'],
        port: typing.Optional[int] = config['minio']['port'],
        protocol: typing.Optional[str] = config['minio']['protocol']
):
    """Perform initial bootstrap of the minIO server

    This function will take care of creating the relevant buckets, groups and
    access controls for using the minIO server for DomiNode.

    """

    for department in utils.get_departments(config):
        typer.echo(f'Bootstrapping department {department!r}...')
        add_department(
            department, access_key, secret_key,
            alias=alias,
            host=host,
            port=port,
            protocol=protocol
        )


def create_group(
        group: str,
        alias: str,
        access_key: str,
        secret_key: str,
        host: str,
        port: int,
        protocol: str = 'https'
) -> typing.Optional[str]:
    minio_kwargs = {
        'alias': alias,
        'access_key': access_key,
        'secret_key': secret_key,
        'host': host,
        'port': port,
        'protocol': protocol
    }
    existing_groups = execute_minio_admin_command('group list', **minio_kwargs)
    for existing in existing_groups:
        if existing.get('name') == group:
            result = group
            break
    else:
        # minio does not allow creating empty groups so we need a user first
        with get_temp_user(**minio_kwargs) as user:
            temp_access_key = user[0]
            creation_result = execute_minio_admin_command(
                'group add',
                **minio_kwargs,
                arguments=f'{group} {temp_access_key}',
            )
            relevant_result = creation_result[0]
            if relevant_result.get('status') == SUCCESS:
                result = group
            else:
                result = None
    return result


def remove_group(
        group: str,
        alias: str,
        access_key: str,
        secret_key: str,
        host: str,
        port: int,
        protocol: str = 'https'
):
    minio_kwargs = {
        'alias': alias,
        'access_key': access_key,
        'secret_key': secret_key,
        'host': host,
        'port': port,
        'protocol': protocol
    }
    removal_result = execute_minio_admin_command(
        'group remove', arguments=group, **minio_kwargs)
    return removal_result[0].get('status') == SUCCESS


def create_temp_user(
        alias: str,
        access_key: str,
        secret_key: str,
        host: str,
        port: int,
        protocol: str = 'https'
) -> typing.Optional[typing.Tuple[str, str]]:
    temp_access_key = 'tempuser'
    temp_secret_key = '12345678'
    minio_kwargs = {
        'alias': alias,
        'access_key': access_key,
        'secret_key': secret_key,
        'host': host,
        'port': port,
        'protocol': protocol
    }
    created = create_user(
        temp_access_key,
        temp_secret_key,
        force=True,
        **minio_kwargs,
    )
    if created:
        result = temp_access_key, temp_secret_key
    else:
        result = None
    return result


@contextmanager
def get_temp_user(
        alias: str,
        access_key: str,
        secret_key: str,
        host: str,
        port: int,
        protocol: str = 'https'
):
    minio_kwargs = {
        'alias': alias,
        'access_key': access_key,
        'secret_key': secret_key,
        'host': host,
        'port': port,
        'protocol': protocol
    }
    user_creds = create_temp_user(**minio_kwargs)
    if user_creds is not None:
        user_access_key, user_secret_key = user_creds
        try:
            yield user_creds
        finally:
            execute_minio_admin_command(
                'user remove',
                arguments=user_access_key,
                **minio_kwargs
            )


def create_user(
        user_access_key: str,
        user_secret_key: str,
        alias: str,
        access_key: str,
        secret_key: str,
        host: str,
        port: int,
        protocol: str = 'https',
        force: bool = False,
) -> bool:
    minio_kwargs = {
        'alias': alias,
        'access_key': access_key,
        'secret_key': secret_key,
        'host': host,
        'port': port,
        'protocol': protocol
    }
    # minio allows overwriting users with the same access_key, so we check if
    # user exists first
    existing_users = execute_minio_admin_command('user list', **minio_kwargs)
    if len(secret_key) < 8:
        raise RuntimeError(
            'Please choose a secret key with 8 or more characters')
    for existing in existing_users:
        if existing.get('accessKey') == user_access_key:
            user_already_exists = True
            break
    else:
        user_already_exists = False
    if not user_already_exists or (user_already_exists and force):
        creation_result = execute_minio_admin_command(
            'user add',
            arguments=f'{user_access_key} {user_secret_key}',
            **minio_kwargs
        )
        result = creation_result[0].get('status') == SUCCESS
    elif user_already_exists:  # TODO: should log that user was not recreated
        result = True
    else:
        result = False
    return result


def execute_command(
        command: str,
        alias: str,
        access_key: str,
        secret_key: str,
        host: str,
        port: int,
        protocol: str = 'https',
        arguments: typing.Optional[str] = None,
):
    full_command = f'mc --json {command} {"/".join((alias, arguments or ""))}'
    typer.echo(full_command)
    parsed_command = shlex.split(full_command)
    process_env = os.environ.copy()
    process_env.update({
        f'MC_HOST_{alias}': (
            f'{protocol}://{access_key}:{secret_key}@{host}:{port}')
    })
    completed = subprocess.run(
        parsed_command,
        capture_output=True,
        env=process_env
    )
    try:
        completed.check_returncode()
    except subprocess.CalledProcessError:
        typer.echo(completed.stdout)
        raise
    result = [json.loads(line) for line in completed.stdout.splitlines()]
    return result


# def old_execute_admin_command(
#         endpoint_alias: str,
#         command: str,
#         arguments: typing.Optional[str] = None,
#         minio_client_config_dir: typing.Optional[Path] = DEFAULT_CONFIG_DIR
# ) -> typing.List:
#     """Uses the ``mc`` binary to perform admin tasks on minIO servers"""
#     full_command = (
#         f'mc --config-dir {minio_client_config_dir} --json admin {command} '
#         f'{endpoint_alias} {arguments or ""}'
#     )
#     parsed_command = shlex.split(full_command)
#     completed = subprocess.run(
#         parsed_command,
#         capture_output=True
#     )
#     try:
#         completed.check_returncode()
#     except subprocess.CalledProcessError:
#         typer.echo(completed.stdout)
#         typer.echo(completed.stderr)
#         raise
#     result = [json.loads(line) for line in completed.stdout.splitlines()]
#     return result


def execute_minio_admin_command(
        command: str,
        alias: str,
        access_key: str,
        secret_key: str,
        host: str,
        port: int,
        protocol: str = 'https',
        arguments: typing.Optional[str] = None,
) -> typing.List:
    """Uses the ``mc`` binary to perform admin tasks on minIO servers"""
    full_command = f'mc --json admin {command} {alias} {arguments or ""}'
    typer.echo(f'Executing admin command: {full_command!r}...')
    parsed_command = shlex.split(full_command)
    process_env = os.environ.copy()
    process_env.update({
        f'MC_HOST_{alias}': (
            f'{protocol}://{access_key}:{secret_key}@{host}:{port}')
    })
    completed = subprocess.run(
        parsed_command,
        capture_output=True,
        env=process_env
    )
    try:
        completed.check_returncode()
    except subprocess.CalledProcessError:
        typer.echo(completed.stdout)
        typer.echo(completed.stderr)
        raise
    result = [json.loads(line) for line in completed.stdout.splitlines()]
    return result


if __name__ == '__main__':
    app()
