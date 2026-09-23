FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1

RUN mkdir -p /app
COPY . /app
WORKDIR /app

RUN chmod +x ./infra/deployment/entrypoint.sh

# build tooling for grpcio + git for the git deps (browseterm-device-control-spec, container-maker-spec)
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="/root/.local/bin:${PATH}"

# infra/deployment.yaml runs this container as a non-root, readOnlyRootFilesystem user
# (runAsUser: 10001) - an in-project venv (/app/.venv, built now while the filesystem is still
# writable) is what actually makes that survivable, not just a chmod on poetry's own default
# /root install: poetry's *default* venv cache path is derived from $HOME, which differs between
# this root build step and the non-root runtime user, so `poetry run` at runtime tries to create
# a brand new venv rather than reusing this one - and fails outright on a read-only filesystem
# with no writable temp dir. Matches browseterm_workload/tunnel_registrar's own Dockerfile, the
# other component under this same runAsUser/readOnlyRootFilesystem constraint - its own entrypoint
# execs /app/.venv/bin/python directly (see this repo's own entrypoint.sh) rather than going
# through `poetry run` at all, for the identical reason.
RUN poetry config virtualenvs.in-project true \
    # browseterm-device-control-spec is pinned to `rev = "main"` (pyproject.toml) - a moving
    # branch ref, deliberately not a fixed commit, since Cloud/Device Agent's protocol spec is
    # meant to track that repo's latest. Plain `poetry lock` does NOT follow a moving branch
    # forward once a commit is already locked for it (poetry only re-resolves what pyproject.toml
    # constraints force it to, and "rev = main" itself never changes) - caught for real: the
    # checked-in lock had resolved_reference pinned to a commit that predates
    # COMMAND_OPERATION_SAVE being added upstream, so the built image's installed package was
    # missing that constant entirely (ImportError at startup) despite GitHub's actual main branch
    # having it. `poetry update` on just this one package forces it to re-resolve to current HEAD.
    && poetry lock && poetry update browseterm-device-control-spec && poetry install --no-root \
    && chmod -R a+rX /app/.venv

ENTRYPOINT ["bash", "-c", "./infra/deployment/entrypoint.sh"]
