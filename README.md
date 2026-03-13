# chicago_team4

## Development setup

### Prerequisites

Install [gitleaks](https://github.com/gitleaks/gitleaks) (required by the local gitleaks pre-commit hook):

```bash
brew install gitleaks   # macOS
```

### Install dependencies

Install development dependencies:

```bash
python -m pip install -r requirements-dev.txt
```

### Enable pre-commit hooks

The following hooks run before each commit: **gitleaks** (secret scanning), **detect-secrets** (secret baseline check), **Black** (formatting), **flake8** (PEP 8 linting), and **mypy** (type checking).

```bash
pre-commit install
pre-commit run --all-files
```

### Run tools manually

```bash
black src
flake8 src
mypy src
```