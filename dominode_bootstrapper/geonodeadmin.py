"""Extra admin commands to manage GeoNode and GeoServer.

This script adds some functions to perform DomiNode related tasks in a more
expedite manner.

"""

import typing
from pathlib import Path

import httpx
import typer

from . import utils
from .constants import (
    GeofenceAccess,
    UserRole,
)

_help_intro = 'Manage GeoNode'

app = typer.Typer(
    short_help=_help_intro,
    help=_help_intro
)
config = utils.load_config()

REPO_ROOT = Path(__file__).resolve().parents[3]
_ANY = '*'
INTERNAL_GEONODE_GROUP_NAME = 'dominode-internal'


class GeoNodeManager:
    client: httpx.Client
    base_url: str
    username: str
    password: str

    def __init__(
            self,
            client: httpx.Client,
            base_url: str = config['geonode']['base_url'],
            username: str = config['geonode']['admin_username'],
            password: str = config['geonode']['admin_password'],
            geoserver_client_id: str = None,
            geoserver_client_secret: str = None,
    ):
        self.client = client
        self.base_url = (
            base_url if not base_url.endswith('/') else base_url.rstrip('/'))
        self.username = username
        self.password = password

    def login(self) -> httpx.Response:
        return self._modify_server_state(
            f'{self.base_url}/account/login/',
            login=self.username,
            password=self.password
        )

    def logout(self) -> httpx.Response:
        return self._modify_server_state(f'{self.base_url}/account/logout/')

    def get_existing_groups(
            self,
            pagination_url: str = None
    ) -> typing.List[typing.Dict]:
        """Retrieve existing groups via GeoNode's REST API"""
        url = pagination_url or f'{self.base_url}/api/group_profile/'
        response = self.client.get(url)
        response.raise_for_status()
        payload = response.json()
        group_profiles: typing.List = payload['objects']
        next_page = payload['meta']['next']
        if next_page is not None:
            group_profiles.extend(self.get_existing_groups(next_page))
        return group_profiles

    def get_group(self, name: str) -> typing.Optional[typing.Dict]:
        response = self.client.get(
            f'{self.base_url}/api/group_profile/',
            params={
                'title': name
            }
        )
        response.raise_for_status()
        matched_groups = response.json().get('objects')
        return matched_groups[0] if len(matched_groups) > 0 else None

    def create_group(self, name: str, description: str) -> httpx.Response:
        """Create a new GeoNode group.

        The GeoNode REST API does not have a way to create new groups. As such,
        as a workaround measure, we impersonate a web browser and create the
        group using the main GUI.

        """

        return self._modify_server_state(
            f'{self.base_url}/groups/create/',
            title=name,
            description=description,
            access='public-invite'
        )

    def add_user(self, username: str, password: str, group: str):
        user_added_response = self._modify_server_state(
            f'{self.base_url}/en/admin/people/profile/add/',
            username=username,
            password1=password,
            password2=password,
            _save='Save'
        )
        user_added_response.raise_for_status()
        added_to_group_response = self._modify_server_state(
            f'{self.base_url}/groups/group/{group}/members_add/',
            csrf_token_url=f'{self.base_url}/groups/group/{group}/members/',
            user_identifiers=username
        )
        added_to_group_response.raise_for_status()


    def _modify_server_state(
            self,
            url: str,
            csrf_token_url: typing.Optional[str] = None,
            **data,
    ):
        """Modify GeoNode state.

        This function is used in the context of making web requests as if we
        were a web browser.

        This function is tailored to the way django CSRF security features
        behave. It first makes a GET request to the specified URL in order to
        retrieve the appropriate CSRF token from the response's cookies. Then
        it makes the actual POST request, with the data to modify the backend.
        This second request sends back the CSRF token, which proves to django
        that the request is legitimate.

        """

        idempotent_response = self.client.get(csrf_token_url or url)
        idempotent_response.raise_for_status()
        request_data = data.copy()
        request_data.update({
            'csrfmiddlewaretoken': idempotent_response.cookies['csrftoken'],
        })
        modifier_response = self.client.post(
            url,
            data=request_data,
            headers={
                'Referer': url,
            },
            cookies=idempotent_response.cookies
        )
        return modifier_response


class GeoServerManager:
    client: httpx.Client
    base_url: str
    headers: dict

    def __init__(
            self,
            client: httpx.Client,
            base_url: str = config['geoserver']['base_url'],
            username: str = config['geoserver']['admin_username'],
            password: str = config['geoserver']['admin_password']
    ):
        self.client = client
        self.base_url = base_url
        self.username = username
        self.password = password
        self.headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

    def list_workspaces(self):

        response = self.client.get(
            f'{self.base_url}/rest/workspaces',
            auth=(self.username, self.password),
            headers=self.headers
        )
        response.raise_for_status()
        return response.json().get('workspaces', {}).get('workspace', [])

    def create_workspace(self, name):

        response = self.client.post(
            f'{self.base_url}/rest/workspaces',
            auth=(self.username, self.password),
            headers=self.headers,
            json={
                    "workspace": {
                        "name": name
                    }
                }
        )
        response.raise_for_status()

    def get_workspace(self, name):

        response = self.client.get(
            f'{self.base_url}/rest/workspaces/{name}',
            auth=(self.username, self.password),
            headers=self.headers
        )
        response.raise_for_status()
        return response.json()

    def create_postgis_store(
            self,
            workspace_name: str,
            store_name: str,
            host: str,
            port: int,
            database: str,
            user: str,
            password: str
    ):
        response = self.client.post(
            f'{self.base_url}/rest/workspaces/{workspace_name}/datastores',
            auth=(self.username, self.password),
            headers=self.headers,
            json={
                'dataStore': {
                    'name': store_name,
                    'connectionParameters': {
                        'entry': [
                            {'@key': 'host', '$': host},
                            {'@key': 'port', '$': str(port)},
                            {'@key': 'database', '$': database},
                            {'@key': 'user', '$': user},
                            {'@key': 'passwd', '$': password},
                            {'@key': 'dbtype', '$': 'postgis'},
                        ]
                    }
                }

            }
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            typer.echo(response.content, err=True)
            typer.echo(str(exc), err=True)
            raise

    def list_geofence_admin_rules(self) -> typing.List:

        response = self.client.get(
            f'{self.base_url}/rest/geofence/adminrules',
            auth=(self.username, self.password),
            headers=self.headers
        )
        response.raise_for_status()
        return response.json().get('rules', [])

    def create_geofence_admin_rule(
            self,
            workspace: str,
            role_name: str,
            role: UserRole,
    ):
        if role == UserRole.EDITOR:
            access = GeofenceAccess.ADMIN
        else:
            access = GeofenceAccess.USER

        response = self.client.post(
            f'{self.base_url}/rest/geofence/adminrules',
            auth=(self.username, self.password),
            headers=self.headers,
            json={
                'AdminRule': {
                    'priority': 0,
                    'roleName': role_name,
                    'workspace': workspace,
                    'access': access.name
                }
            }
        )
        response.raise_for_status()
        return response

    def create_geofence_data_rule(
            self,
            workspace: str,
            role_name: str,
    ):
        headers = self.headers.copy()
        # for some unknown reason the geofence /rules endpoint is not able
        # to cope with the ´application/json´ Accept header (even tough the
        # /adminrules endpoint does not have a problem with it)
        del headers['Accept']
        response = self.client.post(
            f'{self.base_url}/rest/geofence/rules',
            auth=(self.username, self.password),
            headers=headers,
            json={
                'Rule': {
                    'roleName': role_name,
                    'workspace': workspace,
                    'access': 'ALLOW'
                },
            }
        )
        response.raise_for_status()
        return response


@app.command()
def bootstrap(
        geonode_base_url: str = config['geonode']['base_url'],
        geonode_admin_username: str = config['geonode']['admin_username'],
        geonode_admin_password: str = config['geonode']['admin_password'],
        geoserver_base_url: str = config['geoserver']['base_url'],
        geoserver_admin_username: str = config['geoserver']['admin_username'],
        geoserver_admin_password: str = config['geoserver']['admin_password'],
        db_name: typing.Optional[str] = config['db']['name'],
        db_host: str = config['db']['host'],
        db_port: int = config['db']['port'],
):
    """Perform initial bootstrap of GeoNode and GeoServer"""
    with httpx.Client() as client:
        geonode_manager = GeoNodeManager(
            client, geonode_base_url,
            geonode_admin_username, geonode_admin_password
        )
        geonode_manager.login()
        existing_groups = geonode_manager.get_existing_groups()
        existing_group_names = [g.get('title') for g in existing_groups]
        geoserver_manager = GeoServerManager(
            client, geoserver_base_url,
            geoserver_admin_username, geoserver_admin_password
        )
        for department in utils.get_departments(config):
            try:
                geoserver_db_password = config[
                    f'{department}-department']['geoserver_password']
            except KeyError:
                raise RuntimeError(
                    f'Could not retrieve geoserver database user password for '
                    f'department {department}'
                )
            else:
                geonode_group_name = get_geonode_group_name(department)
                if geonode_group_name not in existing_group_names:
                    add_department(
                        department=department,
                        geonode_manager=geonode_manager,
                        geoserver_manager=geoserver_manager,
                        postgis_db_name=db_name,
                        postgis_password=geoserver_db_password,
                        postgis_db_host=db_host,
                        postgis_db_port=db_port
                    )
                else:
                    typer.echo(
                        f'Department {department} has already been '
                        f'bootstrapped, skipping...'
                    )
        typer.echo(f'Creating group {INTERNAL_GEONODE_GROUP_NAME!r}...')
        geonode_manager.create_group(
            INTERNAL_GEONODE_GROUP_NAME,
            'A group for internal DomiNode users'
        )
        geonode_manager.logout()


@app.command()
def add_department_user(
        username: str,
        password: str,
        department: str,
        role: typing.Optional[UserRole] = UserRole.REGULAR_DEPARTMENT_USER,
        geonode_base_url: str = config['geonode']['base_url'],
        geonode_admin_username: str = config['geonode']['admin_username'],
        geonode_admin_password: str = config['geonode']['admin_password'],
):
    with httpx.Client() as http_client:
        manager = GeoNodeManager(
            http_client, geonode_base_url,
            geonode_admin_username, geonode_admin_password
        )
        manager.login()
        if role == UserRole.EDITOR:
            name = get_geonode_group_name(department)
        else:
            name = INTERNAL_GEONODE_GROUP_NAME
        group = manager.get_group(name)
        if group is None:
            raise RuntimeError(f'group {name!r} not found')
        typer.echo(f'Adding user {username!r} to group {group["title"]}...')
        manager.add_user(username, password, group['slug'])
        manager.logout()
    typer.echo('Done!')


def get_geonode_group_name(department: str) -> str:
    return f'{department}-editor'


def get_geoserver_group_name(department: str) -> str:
    return get_geonode_group_name(department).upper()


def add_department(
        department: str,
        geonode_manager: GeoNodeManager,
        geoserver_manager: GeoServerManager,
        postgis_db_name: str,
        postgis_password: str,
        postgis_db_host: str,
        postgis_db_port: int,
):
    geonode_group_name = get_geonode_group_name(department)
    typer.echo(f'Creating geonode group {geonode_group_name!r}...')
    geonode_manager.create_group(
        geonode_group_name,
        description=(
            f'A group for users that are allowed to administer '
            f'{department} datasets'
        )
    )
    _bootstrap_department_in_geoserver(
        geoserver_manager,
        department,
        postgis_user=utils.get_geoserver_db_username(department),
        postgis_password=postgis_password,
        postgis_db_host=postgis_db_host,
        postgis_db_port=postgis_db_port,
        postgis_db_name=postgis_db_name,
    )


def _bootstrap_department_in_geoserver(
        manager: GeoServerManager,
        department: str,
        postgis_user: str,
        postgis_password: str,
        postgis_db_host: str,
        postgis_db_name: str,
        postgis_db_port: typing.Optional[int] = 5432,
):
    """Bootstrap a department in GeoServer

    This function performs the following steps:

    1. create geoserver workspace, in case it does not already exist. If the
       workspace already exists, the function shall return immediately.

    2. Create the relevant geofence admin rules for the workspace -
       The `{department}-editor` group shall be able to administer the
       corresponding workspace.

    3. Create a postgis store in the workspace - this requires using specific
       DB credentials, which provide specific access controls - the DB user
       that is used for each department workspace shall only be able to access
       layers on the **public** schema of the DB AND the user shall only be
       allowed to access layers owned by his own department AND even this
       access must be readonly.

    """

    existing_workspaces = manager.list_workspaces()
    workspace_exists = department in [i['name'] for i in existing_workspaces]
    if not workspace_exists:
        manager.create_workspace(department)
        existing_rules = manager.list_geofence_admin_rules()
        group_name = get_geoserver_group_name(department)
        role_name = f'ROLE_{group_name}'
        typer.echo(f'Existing rules {existing_rules}, role name {role_name}')

        if role_name not in [i['roleName'] for i in existing_rules]:
            typer.echo(f'Creating Geoserver admin rule for {department!r}...')
            manager.create_geofence_admin_rule(
                department, role_name, UserRole.EDITOR)
            typer.echo(f'Creating Geoserver data rule for {department!r}...')
            manager.create_geofence_data_rule(department, role_name)
        manager.create_postgis_store(
            workspace_name=department,
            store_name=f"dominode_db_{department}",
            host=postgis_db_host,
            port=postgis_db_port,
            database=postgis_db_name,
            user=postgis_user,
            password=postgis_password
        )
    else:
        typer.echo(f'Workspace {department!r} already exists, skipping...')

