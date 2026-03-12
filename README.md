# chicago_team4

## Development setup

Install development dependencies (including Black):

```bash
python -m pip install -r requirements-dev.txt
```

Run Black manually:

```bash
black src
```

Enable pre-commit hooks so Black runs before each commit:

```bash
pre-commit install
pre-commit run --all-files
```