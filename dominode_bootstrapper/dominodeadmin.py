"""Extra commands to manage the DomiNode system"""

import typer
import typing

from . import (
    dbadmin,
    geonodeadmin,
    minioadmin,
    utils,
)

app = typer.Typer()
app.add_typer(dbadmin.app, name='db')
app.add_typer(geonodeadmin.app, name='geonode')
app.add_typer(minioadmin.app, name='minio')

config = utils.load_config()


@app.command()
def bootstrap(
        db_admin_username: typing.Optional[str] = config[
            'db']['admin_username'],
        db_admin_password: typing.Optional[str] = config[
            'db']['admin_password'],
        db_name: typing.Optional[str] = config['db']['name'],
        db_host: typing.Optional[str] = config['db']['host'],
        db_port: typing.Optional[int] = config['db']['port'],
        minio_admin_access_key: typing.Optional[str] = config[
            'minio']['admin_access_key'],
        minio_admin_secret_key: typing.Optional[str] = config[
            'minio']['admin_secret_key'],
        minio_alias: typing.Optional[str] = 'dominode_bootstrapper',
        minio_host: typing.Optional[str] = config['minio']['host'],
        minio_port: typing.Optional[int] = config['minio']['port'],
        minio_protocol: typing.Optional[str] = config['minio']['protocol']
):
    typer.echo('Bootstrapping DomiNode database...')
    dbadmin.bootstrap(
        db_admin_username=db_admin_username,
        db_admin_password=db_admin_password,
        db_name=db_name,
        db_host=db_host,
        db_port=db_port
    )
    typer.echo('Bootstrapping DomiNode minIO...')
    minioadmin.bootstrap(
        minio_admin_access_key,
        minio_admin_secret_key,
        minio_alias,
        minio_host,
        minio_port,
        minio_protocol
    )
    geonodeadmin.bootstrap(
        geonode_base_url=config['geonode']['base_url'],
        geonode_username=config['geonode']['admin_username'],
        geonode_password=config['geonode']['admin_password'],
        geoserver_base_url=config['geoserver']['base_url'],
        geoserver_username=config['geoserver']['admin_username'],
        geoserver_password=config['geoserver']['admin_password']
    )
    typer.echo('Done!')