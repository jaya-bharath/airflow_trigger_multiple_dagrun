# airflow-trigger-multiple-dagrun

## Usage

```shell
pip install airflow-trigger-multiple-dagrun
```

## Dev setup

### initial setup

```shell
pip install poetry
make setup
```

### publish to testpypi

```shell
poetry config repositories.testpypi https://test.pypi.org/legacy/
poetry publish --build -r testpypi -p <password> -u <username:=__token__>
```

### useful poetry commands

```shell
poetry add <dependencies>
poetry add --dev <dependencies> # dev dependencies
poetry update
poetry lock --no-update
```

## Contribution

Contributions are very welcome. Tests can be run with `make test`, please ensure the coverage at least stays the same
before you submit a pull request.

## License

Distributed under the terms of the `MIT` license, "airflow-trigger-multiple-dagrun" is free and open source software



