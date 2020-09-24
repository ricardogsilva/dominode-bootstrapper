import os
import typing
from configparser import ConfigParser
from pathlib import Path

import typer


def _get_default_config_paths() -> typing.Tuple:
    result = [
        Path('/etc/dominode/dominode-bootstrapper.conf'),
        Path(typer.get_app_dir('dominode-bootstrapper')) / 'config.conf',
    ]
    from_env_path = os.getenv('DOMINODE_BOOTSTRAPPER_CONFIG_PATH')
    if from_env_path:
        result.append(Path(from_env_path))
    return tuple(result)


def load_config(
        paths: typing.Optional[
            typing.Iterable[typing.Union[str, Path]]
        ] = _get_default_config_paths()
) -> ConfigParser:
    """Load configuration values

    Config is composed by looking for values in multiple places:

    - Default config values, as specified in the ``_get_default_config()``
      function

    - The following paths, if they exist:
      - /etc/dominode/.dominode-bootstrapper.conf
      - $HOME/.config/dominode-bootstrapper/config.conf
      - whatever file is specified by the DOMINODE_BOOTSTRAPPER_CONFIG_PATH
        environment variable

    - Environment variables named like `DOMINODE__{SECTION}__{KEY}`

    """

    config = _get_default_config()
    config.read(paths)
    for section, section_options in get_config_from_env().items():
        for key, value in section_options.items():
            try:
                config[section][key] = value
            except KeyError:
                config[section] = {key: value}
    return config


def get_config_from_env(
        environment: typing.Optional[typing.Dict] = os.environ
) -> typing.Dict[str, typing.Dict[str, str]]:
    result = {}
    for key, value in environment.items():
        if key.startswith('DOMINODE__DEPARTMENT__'):
            try:
                department, config_key = key.split('__')[2:]
            except ValueError:
                typer.echo(f'Could not read variable {key}, ignoring...')
                continue
            section_name = f'{department.lower()}-department'
            department_section = result.setdefault(section_name, {})
            department_section[config_key.lower()] = value
        elif key.startswith('DOMINODE__'):
            try:
                section, config_key = [i.lower() for i in key.split('__')[1:]]
            except ValueError:
                typer.echo(f'Could not read variable {key}, ignoring...')
                continue
            conf_section = result.setdefault(section, {})
            conf_section[config_key] = value
    return result


def _get_default_config():
    config = ConfigParser()
    config['db'] = {}
    config['db']['name'] = 'postgres'
    config['db']['host'] = 'localhost'
    config['db']['port'] = '5432'
    config['db']['admin_username'] = 'postgres'
    config['db']['admin_password'] = 'postgres'
    config['minio'] = {}
    config['minio']['host'] = 'localhost'
    config['minio']['port'] = '9000'
    config['minio']['protocol'] = 'https'
    config['minio']['admin_access_key'] = 'admin'
    config['minio']['admin_secret_key'] = 'admin'
    config['geonode'] = {}
    config['geonode']['base_url'] = 'http://localhost'
    config['geonode']['admin_username'] = 'admin'
    config['geonode']['admin_password'] = 'admin'
    config['geoserver'] = {}
    config['geoserver']['base_url'] = 'http://localhost/geoserver'
    config['geoserver']['admin_username'] = 'admin'
    config['geoserver']['admin_password'] = 'geoserver'
    config['dominode'] = {}
    config['dominode']['generic_user_name'] = 'dominode_user'
    config['dominode']['generic_editor_role_name'] = 'editor'
    default_departments = (
        'ppd',
        'lsd',
    )
    for department in default_departments:
        section_name = f'{department}-department'
        config[section_name] = {}
        config[section_name]['geoserver_password'] = 'dominode'
    return config


def get_departments(config: ConfigParser) -> typing.List[str]:
    separator = '-'
    result = []
    for section in config.sections():
        if section.endswith(f'{separator}department'):
            result.append(section.partition(separator)[0])
    return result


def get_geoserver_db_username(department: str):
    return f'{department}_geoserver'
