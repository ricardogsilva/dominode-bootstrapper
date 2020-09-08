FROM python:3.8-buster

RUN curl --silent --show-error --location \
    https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py > /opt/get-poetry.py \
  && curl --silent --show-error --location \
    https://dl.min.io/client/mc/release/linux-amd64/archive/mc.RELEASE.2020-08-20T00-23-01Z > /usr/local/bin/mc \
  && chmod +x /usr/local/bin/mc

RUN adduser --gecos "" --disabled-password dominode

USER dominode

WORKDIR /home/dominode

RUN python /opt/get-poetry.py --yes --version 1.0.10

ENV PATH="$PATH:/home/dominode/.poetry/bin"

COPY --chown=dominode:dominode pyproject.toml poetry.lock dominode-bootstrapper/

WORKDIR /home/dominode/dominode-bootstrapper

RUN poetry install --no-root

COPY --chown=dominode:dominode . .

RUN poetry install

ENTRYPOINT ["poetry", "run", "dominode-admin"]
