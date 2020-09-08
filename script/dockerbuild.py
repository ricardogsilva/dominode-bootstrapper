"""Build (and push) dominode-bootstrapper docker image"""

import typing
from pathlib import Path
from subprocess import check_output

import docker
import typer
from docker.errors import BuildError

app = typer.Typer()

DOCKERFILE = Path(__file__).resolve().parents[1] / 'Dockerfile'


@app.command()
def main(
        repository: str = 'dominode-bootstrapper',
        registry_url: typing.Optional[str] = 'https://index.docker.io/v1/',
        username: typing.Optional[str] = None,
        password: typing.Optional[str] = None,
):
    client = docker.from_env()
    tag = get_version()
    build_tag = f'{repository}:{tag}'
    typer.echo(f'Building {build_tag!r}...')
    try:
        image, logs = client.images.build(
            path=str(DOCKERFILE.parent),
            tag=build_tag,
            rm=True,
            pull=True,
            dockerfile='Dockerfile',
        )
        for json_log in logs:
            for value in json_log.values():
                typer.echo(value)
    except BuildError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1)
    if username is not None:
        client.login(username, password, registry=registry_url)
        typer.echo(f'Pushing {build_tag!r} to {registry_url!r}...')
        client.images.push(repository, tag)
    typer.echo('Done!')


def get_version():
    return check_output(
        ['poetry', 'version']).strip().decode('utf-8').split()[-1]


if __name__ == '__main__':
    app()