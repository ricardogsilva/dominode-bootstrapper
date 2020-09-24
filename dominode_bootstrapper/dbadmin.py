"""Extra admin commands to manage the DomiNode database server

This script adds some functions to perform DomiNode related tasks in a more
expedite manner than using the bare `psql` client


ricardosilva
lsduser1
ppduser1

INSERT INTO lsd_topomaps.qgis_projects
SELECT * FROM lsd_staging.qgis_projects WHERE name = 'this'

"""

import typing
from contextlib import contextmanager
from pathlib import Path
from time import sleep

import sqlalchemy as sla
import typer
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql import text
from sqlalchemy.engine import Connection

from .constants import UserRole
from . import utils

_help_intro = 'Manage postgis database'

app = typer.Typer(
    short_help=_help_intro,
    help=_help_intro
)

APP_ROOT = Path(__file__).resolve().parents[1]
config = utils.load_config()
LSD_TOPOMAP_EDITOR_ROLE_NAME = f'lsd_topomap_editor'


@app.command()
def bootstrap(
        db_admin_username: typing.Optional[str] = config['db']['admin_username'],
        db_admin_password: typing.Optional[str] = config['db']['admin_password'],
        db_name: typing.Optional[str] = config['db']['name'],
        db_host: str = config['db']['host'],
        db_port: int = config['db']['port'],
):
    """Perform initial bootstrap of the database

    This function will take care of creating the relevant schemas, group roles
    and access controls for using the postgis database for DomiNode.

    """

    db_url = get_db_url(
        db_admin_username, db_admin_password, db_host, db_port, db_name)
    dominode_staging_schema_name = 'dominode_staging'
    with get_db_connection(db_url) as db_connection:
        typer.echo('Creating general roles...')
        create_role(
            'admin', db_connection, other_options=('CREATEDB', 'CREATEROLE'))
        create_role(
            'replicator', db_connection, other_options=('REPLICATION',))
        create_role(config['dominode']['generic_user_name'], db_connection)

        create_role(
            config['dominode']['generic_editor_role_name'],
            db_connection,
            other_options=('IN ROLE dominode_user', )
        )
        typer.echo(f'creating {dominode_staging_schema_name!r} schema...')
        create_schema(
            dominode_staging_schema_name,
            config['dominode']['generic_editor_role_name'],
            db_connection
        )
        typer.echo(
            f'Granting permissions on {dominode_staging_schema_name!r} '
            f'schema...'
        )
        grant_schema_permissions(
            dominode_staging_schema_name,
            ('USAGE', 'CREATE'),
            config['dominode']['generic_user_name'], db_connection
        )
        for department in utils.get_departments(config):
            typer.echo(f'Bootstrapping {department} department...')
            bootstrap_department(
                db_connection,
                department,
                config['dominode']['generic_user_name']
            )

        typer.echo(f'Modifying access permissions on public schema...')
        db_connection.execute(
            text('REVOKE CREATE ON SCHEMA public FROM public'))
        db_connection.execute(
            text(
                f'GRANT CREATE ON SCHEMA public TO '
                f'{config["dominode"]["generic_editor_role_name"]}'
            ),
        )
        typer.echo(f'Executing remaining SQL commands...')
        raw_connection = db_connection.connection
        raw_cursor = raw_connection.cursor()
        bootstrap_sql_path = APP_ROOT / 'sql/finalize-bootstrap-db.sql'
        raw_cursor.execute(bootstrap_sql_path.read_text())
        raw_connection.commit()


@app.command()
def add_department(
        department: str,
        db_admin_username: typing.Optional[str] = config['db']['admin_username'],
        db_admin_password: typing.Optional[str] = config['db']['admin_password'],
        db_name: typing.Optional[str] = config['db']['name'],
        db_host: str = config['db']['host'],
        db_port: int = config['db']['port'],
):
    db_url = get_db_url(
        db_admin_username, db_admin_password, db_host, db_port, db_name)
    with get_db_connection(db_url) as db_connection:
        bootstrap_department(
            db_connection,
            department,
            config['dominode']['generic_user_name']
        )


@app.command()
def add_department_user(
        username: str,
        password: str,
        departments: typing.List[str],
        role: typing.Optional[UserRole] = UserRole.REGULAR_DEPARTMENT_USER,
        is_topomap_editor: typing.Optional[bool] = False,
        db_admin_username: typing.Optional[str] = config['db']['admin_username'],
        db_admin_password: typing.Optional[str] = config['db']['admin_password'],
        db_host: typing.Optional[str] = config['db']['host'],
        db_port: typing.Optional[int] = config['db']['port'],
        db_name: typing.Optional[str] = config['db']['name'],
):
    db_url = get_db_url(
        db_admin_username, db_admin_password, db_host, db_port, db_name)
    role_suffix = 'editor' if role == UserRole.EDITOR else 'user'
    parent_roles = [f'{dep}_{role_suffix}' for dep in departments]
    if is_topomap_editor:
        parent_roles.append(LSD_TOPOMAP_EDITOR_ROLE_NAME)
    typer.echo(
        f'Adding {username!r} user with parent roles {parent_roles!r}...')
    with get_db_connection(db_url) as db_connection:
        create_user(
            username, password, db_connection, parent_roles=parent_roles)
    typer.echo('Done!')


def create_role(
        name: str,
        connection: Connection,
        parent_roles: typing.Optional[typing.Iterable[str]] = None,
        other_options: typing.Optional[typing.Tuple] = None,
):
    if not check_for_role(name, connection):
        options = f'WITH'
        if parent_roles is not None:
            options = f'{options} IN ROLE {", ".join(parent_roles)}'
        if other_options is not None:
            options = f'{options} {" ".join(other_options)}'
        connection.execute(
            text(f'CREATE ROLE {name} {options}')
        )
    else:
        typer.echo(f'Role {name!r} already exists. Skipping...')


def create_user(
        name: str,
        password: str,
        connection: Connection,
        parent_roles: typing.Optional[typing.Iterable[str]] = None
):
    return create_role(
        name,
        connection,
        parent_roles=parent_roles,
        other_options=(
            'LOGIN',
            f'PASSWORD \'{password}\''
        )
    )


def check_for_role(role: str, connection: Connection) -> bool:
    """Check if role exists on the database"""
    return connection.execute(
        text('SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = :role)'),
        role=role
    ).scalar()


def create_schema(name: str, owner_role: str, connection: Connection):
    return connection.execute(
        text(f'CREATE SCHEMA IF NOT EXISTS {name} AUTHORIZATION {owner_role}')
    )


def grant_schema_permissions(
        schema: str,
        permissions: typing.Iterable[str],
        role: str,
        connection: Connection
):
    return connection.execute(
        text(f'GRANT {", ".join(permissions)} ON SCHEMA {schema} TO {role}')
    )


@contextmanager
def get_db_connection(db_url: str):
    engine = sla.create_engine(db_url)
    connected = False
    max_tries = 30
    current_try = 0
    sleep_for = 2  # seconds
    while not connected and current_try < max_tries:
        try:
            with engine.connect() as connection:
                connected = True
                yield connection
        except OperationalError:
            print(f'Could not connect to DB ({current_try + 1}/{max_tries})')
            current_try += 1
            if current_try < max_tries:
                sleep(sleep_for)
            else:
                raise


def bootstrap_department(
        db_connection,
        department: str,
        generic_user_name: str
):
    user_role = f'{department}_user'
    editor_role = f'{department}_editor'
    typer.echo(f'Creating role {user_role!r}...')
    create_role(
        user_role, db_connection, parent_roles=('dominode_user',))
    typer.echo(f'Creating role {editor_role!r}...')
    create_role(
        editor_role, db_connection, parent_roles=('editor', user_role))
    staging_schema_name = f'{department}_staging'
    typer.echo(f'Creating {staging_schema_name!r} schema...')
    create_schema(staging_schema_name, editor_role, db_connection)
    typer.echo(f'Setting schema permissions...')
    grant_schema_permissions(
        staging_schema_name,
        ('USAGE', 'CREATE'),
        user_role,
        db_connection
    )
    typer.echo(f'Creating qgis_projects table...')
    create_qgis_projects_table(db_connection, staging_schema_name, user_role)
    typer.echo(f'Adding GeoServer user account...')
    geoserver_password = config[
        f'{department}-department']['geoserver_password']
    create_user(
        utils.get_geoserver_db_username(department),
        geoserver_password,
        db_connection,
        parent_roles=[generic_user_name]
    )
    if department == 'lsd':
        bootstrap_lsd_topomaps(db_connection, user_role)


def create_qgis_projects_table(
        db_connection,
        schema: str,
        owner: str,
        revoke_owner_updates: typing.Optional[bool] = False,
        grant_select_to: typing.Optional[str] = None,
):
    table_qualified_name = f'{schema}.qgis_projects'
    db_connection.execute(
        text(
            f'CREATE TABLE IF NOT EXISTS {table_qualified_name} ('
            f'name text not null constraint qgis_projects_pkey primary key ,'
            f'metadata jsonb,'
            f'content bytea'
            f')'
        )
    )
    db_connection.execute(
        text(f'ALTER TABLE {table_qualified_name} OWNER TO {owner}')
    )
    if revoke_owner_updates:
        db_connection.execute(
            text(f'REVOKE UPDATE ON {table_qualified_name} FROM {owner}')
        )
    if grant_select_to:
        db_connection.execute(
            text(
                f'GRANT SELECT ON {table_qualified_name} TO {grant_select_to}')
        )


def bootstrap_lsd_topomaps(db_connection, user_role_name: str):
    typer.echo(f'Creating role {LSD_TOPOMAP_EDITOR_ROLE_NAME!r}...')
    create_role(
        LSD_TOPOMAP_EDITOR_ROLE_NAME,
        db_connection,
        parent_roles=(user_role_name,)
    )
    schema_name = f'lsd_topomaps'
    typer.echo(f'Creating {schema_name!r} schema...')
    create_schema(
        schema_name,
        LSD_TOPOMAP_EDITOR_ROLE_NAME,
        db_connection
    )
    typer.echo(f'Setting permissions on schema {schema_name!r}...')
    grant_schema_permissions(
        schema_name,
        ('USAGE',),
        user_role_name,
        db_connection
    )
    create_qgis_projects_table(
        db_connection,
        schema_name,
        LSD_TOPOMAP_EDITOR_ROLE_NAME,
        revoke_owner_updates=True,
        grant_select_to=user_role_name
    )


def get_db_url(
        username: str,
        password: str,
        host: str,
        port: typing.Union[str, int],
        db_name: typing.Optional[str] = None
) -> str:
    return (
        f'postgresql://{username}:{password}@'
        f'{host}:{port}/{db_name or username}'
    )
