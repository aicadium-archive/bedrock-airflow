## bedrock-airflow development

## Developing

To start developing your own airflow operators for `bedrock_plugin`, you will need to install the required development dependencies.
We recommend doing it in a virtual environment to avoid potential dependency conflicts. For example,

```bash
$ python3 -m venv venv
$ source venv/bin/activate
$ pip install -r requirements-dev.txt
```

## Running the airflow docker

You can start a local airflow setup using the provided docker-compose file. The web UI will be available at [http://localhost:8888](http://localhost:8888). The plugin and example DAG put into the docker container is symlinked to those in the repo. The DB is persisted for convenience; to nuke it delete the `./pgdata` directory.

You need to symlink the plugin and example DAG into `plugins` and `dags`.

```bash
ln -s ../examples/bedrock_dag.py ./dags/bedrock_dag.symlink.py
ln -s ../bedrock_plugin.py ./plugins/bedrock_plugin.symlink.py

docker-compose up
```

Update `docker-compose.override.yml` with credentials for your git repository (this file is gitignored, so you need to copy the template).

### Running unit tests

Once your local development environment is setup, you can verify that everything is working by executing our unit tests with `pytest`.

```bash
$ pytest -v tests
```
