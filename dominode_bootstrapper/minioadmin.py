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
        f'credentials of the minIO server that you want to use. Check out the '
        f'minIO client docs at: \n\n'
        f'https://docs.min.io/docs/minio-client-quickstart-guide.html\n\n'
        f'for details on how to download mc and configure it.'
    )
)

SUCCESS = "success"
DEFAULT_CONFIG_DIR = Path('~/.mc').expanduser()
DOMINODE_STAGING_BUCKET_NAME: str = 'dominode-staging'
PUBLIC_BUCKET_NAME: str = 'public'
POLICY_VERSION: str = '2012-10-17'

config = utils.load_config()


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

    This function performs the following:

    - create a common staging bucket
    - create a public bucket
    - set anonymous policy for public bucket to dowload only
    - add default departments

    """

    manager = MinioManager(alias, access_key, secret_key, host, port, protocol)
    manager.create_bucket(DOMINODE_STAGING_BUCKET_NAME)
    manager.create_bucket(PUBLIC_BUCKET_NAME)
    manager.set_anonymous_policy(PUBLIC_BUCKET_NAME)
    typer.echo(f'Bootstrapping departments...')
    for department in utils.get_departments(config):
        add_department(
            department,
            access_key,
            secret_key,
            alias,
            host,
            port,
            protocol,
        )


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
    """Add a new department after the initial bootstrap has been done

    This includes:

    -  Creating a department staging bucket
    -  Creating a directory for the department in the common staging bucket
    -  Creating a directory for the department in the public bucket

    """

    manager = MinioManager(alias, access_key, secret_key, host, port, protocol)
    staging_bucket = get_staging_bucket_name(name)
    typer.echo(f'Creating {staging_bucket!r} bucket...')
    manager.create_bucket(staging_bucket)
    staging_dir = get_dominode_staging_root_dir_name(name)
    typer.echo(f'Creating {staging_dir!r} dir...')
    manager.create_bucket(f'{staging_dir}')
    public_dir = get_public_root_dir_name(name)
    typer.echo(f'Creating {public_dir!r} dir...')
    manager.create_bucket(f'{public_dir}')


@app.command()
def add_department_user(
        user_access_key: str,
        user_secret_key: str,
        departments: typing.List[str],
        role: typing.Optional[UserRole] = UserRole.REGULAR_DEPARTMENT_USER,
        access_key: typing.Optional[str] = config['minio']['admin_access_key'],
        secret_key: typing.Optional[str] = config['minio']['admin_secret_key'],
        alias: str = 'dominode_bootstrapper',
        host: typing.Optional[str] = config['minio']['host'],
        port: typing.Optional[int] = config['minio']['port'],
        protocol: typing.Optional[str] = config['minio']['protocol']

):
    """Create a user and add it to the relevant departments

    This function shall ensure that when a new user is created it is put in the
    relevant groups and with the correct access policies

    """

    manager = MinioManager(alias, access_key, secret_key, host, port, protocol)
    added = manager.add_user(user_access_key, user_secret_key)
    if not added:
        raise RuntimeError(f'Could not add user {user_access_key}')

    policy_name = get_policy_name(role, departments)
    group_name = get_group_name(role, policy_name)
    if not manager.policy_exists(policy_name):
        policy_generator = {
            UserRole.REGULAR_DEPARTMENT_USER: get_user_policy,
            UserRole.EDITOR: get_editor_policy,
        }[role]
        typer.echo(f'Generating access policy {policy_name!r}...')
        policy = policy_generator(departments)
        typer.echo(f'Adding policy to the server...')
        manager.add_policy(policy_name, policy)
        typer.echo(f'Generating group {group_name!r}...')
        manager.add_group(group_name)
        typer.echo(f'Setting policy {policy_name!r} on group {group_name!r}...')
        manager.set_policy(policy_name, group=group_name)
    typer.echo(f'Adding user {user_access_key!r} to {group_name!r}...')
    manager.add_user_to_group(user_access_key, group_name)


class MinioManager:
    endpoint_alias: str
    access_key: str
    secret_key: str
    host: str
    port: int
    protocol: str

    def __init__(
            self,
            endpoint_alias: str,
            access_key: str,
            secret_key: str,
            host: str,
            port: int = 9000,
            protocol: str = 'https',
    ):
        self.endpoint_alias = endpoint_alias
        self.host = host
        self.port = port
        self.protocol = protocol
        self.access_key = access_key
        self.secret_key = secret_key

    def add_group(self, name: str):
        return create_group(
            name,
            self.endpoint_alias,
            self.access_key,
            self.secret_key,
            self.host,
            self.port,
            self.protocol
        )

    def create_bucket(self, name: str):
        extra = '--ignore-existing'
        self._execute_command('mb', f'{name} {extra}')

    def set_anonymous_policy(self, bucket: str):
        self._execute_command('policy set download', bucket)

    def add_user(
            self,
            access_key: str,
            secret_key: str,
    ) -> bool:
        typer.echo(f'Creating user {access_key!r}...')
        return create_user(
            access_key,
            secret_key,
            alias=self.endpoint_alias,
            access_key=self.access_key,
            secret_key=self.secret_key,
            host=self.host,
            port=self.port,
            protocol=self.protocol,
        )

    def add_user_to_group(self, user: str, group: str) -> bool:
        addition_result = self._execute_admin_command(
            'group add',
            f'{group} {user}'
        )
        return addition_result[0].get('status') == SUCCESS

    def policy_exists(self, name: str) -> bool:
        """Check if a policy already exists"""
        existing_policies = self._execute_admin_command('policy list')
        for item in existing_policies:
            if item.get('policy') == name:
                result = True
                break
        else:
            result = False
        return result

    def add_policy(self, name: str, policy: typing.Dict):
        """Add policy to the server"""
        os_file_handler, pathname = tempfile.mkstemp(text=True)
        with os.fdopen(os_file_handler, mode='w') as fh:
            json.dump(policy, fh)
        self._execute_admin_command(
            'policy add',
            f'{name} {pathname}',
        )
        Path(pathname).unlink(missing_ok=True)

    def set_policy(
            self,
            policy: str,
            user: typing.Optional[str] = None,
            group: typing.Optional[str] = None,
    ):
        if user is None and group is None:
            raise RuntimeError('Must provide either `user` or `group`')
        elif user is not None:
            suffix = f'user={group}'
        else:
            suffix = f'group={group}'
        self._execute_admin_command(
            'policy set',
            f'{policy} {suffix}',
        )

    def _execute_command(
            self,
            command: str,
            arguments: typing.Optional[str] = None,
    ):
        return execute_command(
            command,
            alias=self.endpoint_alias,
            access_key=self.access_key,
            secret_key=self.secret_key,
            host=self.host,
            port=self.port,
            protocol=self.protocol,
            arguments=arguments,
        )

    def _execute_admin_command(
            self,
            command: str,
            arguments: typing.Optional[str] = None,
    ):
        return execute_minio_admin_command(
            command,
            alias=self.endpoint_alias,
            access_key=self.access_key,
            secret_key=self.secret_key,
            host=self.host,
            port=self.port,
            protocol=self.protocol,
            arguments=arguments,
        )


def get_user_policy(departments: typing.List[str]):
    """Generate a policy for a user role for input departments"""
    policy = {
        'Version': POLICY_VERSION,
        'Statement': [
            {
                'Sid': f'regular-user-deny-bucket-delete',
                'Action': [
                    's3:DeleteBucket',
                ],
                'Effect': 'Deny',
                'Resource': [
                    f'arn:aws:s3:::{DOMINODE_STAGING_BUCKET_NAME}',
                    # add all department staging buckets (see below)
                ]
            },
            {
                'Sid': f'regular-user-allow-full-access',
                'Action': [
                    's3:*'
                ],
                'Effect': 'Allow',
                'Resource': [
                    # add all staging buckets (see below)
                    # add all subdirs under dominode_staging according to input departments (see below)
                    # add all subdirs under public according to input departments (see below)
                ]
            },
            {
                'Sid': f'regular-user-read-only-access',
                'Action': [
                    's3:GetBucketLocation',
                    's3:ListBucket',
                    's3:GetObject',
                ],
                'Effect': 'Allow',
                'Resource': [
                    f'arn:aws:s3:::{DOMINODE_STAGING_BUCKET_NAME}/*',
                    f'arn:aws:s3:::{PUBLIC_BUCKET_NAME}/*'
                ]
            },
        ]
    }
    deny_bucket_index = 0
    allow_full_access_index = 1
    for department in departments:
        # tweak statement ids to make them unique
        for statement in policy['Statement']:
            statement['Sid'] += f'-{department}'

        # now add relevant access permissions
        policy['Statement'][deny_bucket_index]['Resource'].append(
            f'arn:aws:s3:::{get_staging_bucket_name(department)}',
        )
        policy['Statement'][allow_full_access_index]['Resource'].extend([
            f'arn:aws:s3:::{get_staging_bucket_name(department)}/*',
            f'arn:aws:s3:::{get_dominode_staging_root_dir_name(department)}*',
        ])
    return policy


def get_editor_policy(departments: typing.List[str]):
    """Generate a policy for an editor role for input departments"""
    policy = {
        'Version': POLICY_VERSION,
        'Statement': [
            {
                'Sid': f'editor-user-deny-bucket-delete',
                'Action': [
                    's3:DeleteBucket',
                ],
                'Effect': 'Deny',
                'Resource': [
                    f'arn:aws:s3:::{DOMINODE_STAGING_BUCKET_NAME}',
                    # add all department staging buckets
                ]
            },
            {
                'Sid': f'editor-user-allow-full-access',
                'Action': [
                    's3:*'
                ],
                'Effect': 'Allow',
                'Resource': [
                    # add all staging buckets (see below)
                    # add all subdirs under dominode_staging according to input departments (see below)
                    # add all subdirs under public according to input departments (see below)
                ]
            },
            {
                'Sid': f'editor-user-read-only-access',
                'Action': [
                    's3:GetBucketLocation',
                    's3:ListBucket',
                    's3:GetObject',
                ],
                'Effect': 'Allow',
                'Resource': [
                    f'arn:aws:s3:::{DOMINODE_STAGING_BUCKET_NAME}/*',
                    f'arn:aws:s3:::{PUBLIC_BUCKET_NAME}/*'
                ]
            },
        ]
    }
    deny_bucket_index = 0
    allow_full_access_index = 1
    for department in departments:
        # tweak statement ids to make them unique
        for statement in policy['Statement']:
            statement['Sid'] += f'-{department}'

        # now add relevant access permissions
        policy['Statement'][deny_bucket_index]['Resource'].append(
            f'arn:aws:s3:::{get_staging_bucket_name(department)}',
        )
        policy['Statement'][allow_full_access_index]['Resource'].extend([
            f'arn:aws:s3:::{get_staging_bucket_name(department)}/*',
            f'arn:aws:s3:::{get_dominode_staging_root_dir_name(department)}*',
            f'arn:aws:s3:::{get_public_root_dir_name(department)}*',
        ])
    return policy


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


def get_group_name(role: UserRole, policy: str) -> str:
    suffix = {
        UserRole.REGULAR_DEPARTMENT_USER: '-user-group',
        UserRole.EDITOR: '-editor-group',
    }[role]
    return f'{policy}{suffix}'


def get_staging_bucket_name(department: str) -> str:
    return f'{department}-staging'


def get_dominode_staging_root_dir_name(department: str) -> str:
    return f'{DOMINODE_STAGING_BUCKET_NAME}/{department}/'


def get_public_root_dir_name(department: str) -> str:
    return f'{PUBLIC_BUCKET_NAME}/{department}/'


def get_policy_name(role: UserRole, departments: typing.List[str]) -> str:
    dept_names = '-'.join(sorted(departments))
    return {
        UserRole.REGULAR_DEPARTMENT_USER: (
            f'{dept_names}-regular-user-group-policy'),
        UserRole.EDITOR: f'{dept_names}-editor-group-policy',
    }[role]


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
