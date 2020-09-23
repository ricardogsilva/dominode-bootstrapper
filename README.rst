*********************
DomiNode bootstrapper
*********************

A command line utility to perform bootstrapping of several DomiNode-related apps.

=======
Install
=======

This uses `poetry`_, so clone this project, install poetry and then::

    cd dominode-bootstrapper
    poetry install


=====
Usage
=====

This package installs the `dominode-admin` CLI tool. Use it like this::

    poetry run dominode-admin --help


-------------------------------------
Bootstrapping DomiNode infrastructure
-------------------------------------

In order to bootstrap the DomiNode infrastructure either::

    poetry run dominode-admin bootstrap


Or bootstrap each resource individually::

    poetry run dominode-admin db bootstrap
    poetry run dominode-admin minio bootstrap



=============
Running tests
=============

This package uses `pytest`_, so to run tests::

    poetry run pytest

.. _poetry: https://python-poetry.org/
.. _pytest: https://docs.pytest.org/en/latest/


======================
Build the docker image
======================

In order to build the docker image::

    docker build -t kartoza/dominode_bootstrapper:$(poetry version | cut -d " " -f 2) .
