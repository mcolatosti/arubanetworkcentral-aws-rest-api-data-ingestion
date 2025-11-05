# Python-only Aruba Central CDK Ingestion

VENV = .venv
PY = $(VENV)/bin/python
PIP = $(VENV)/bin/pip
CDK ?= cdk   # Assumes AWS CDK CLI installed globally (npm i -g aws-cdk)

.DEFAULT_GOAL := help

.PHONY: help venv install synth diff deploy deploy-prod destroy clean logs test local-run format

help:
    @echo "Targets:"
    @echo "  venv        - Create virtual environment"
    @echo "  install     - Install Python deps (infra + runtime + tests)"
    @echo "  synth       - cdk synth"
    @echo "  diff        - cdk diff"
    @echo "  deploy      - Deploy (dev)"
    @echo "  deploy-prod - Deploy (prod)"
    @echo "  destroy     - Destroy current dev stack"
    @echo "  logs        - Tail Lambda logs"
    @echo "  test        - Run pytest"
    @echo "  local-run   - Run ingestion locally (needs env + secret JSON files)"
    @echo "  format      - Auto-format (requires 'black')"
    @echo "  clean       - Remove build artifacts and venv"

venv:
    @test -d $(VENV) || python3 -m venv $(VENV)
    @$(PIP) install --upgrade pip >/dev/null

install: venv
    $(PIP) install -r requirements.txt

synth: install
    $(CDK) synth

diff: install
    $(CDK) diff

deploy: install
    $(CDK) deploy --context environment=dev --require-approval never

deploy-prod: install
    $(CDK) deploy --context environment=prod

destroy: install
    $(CDK) destroy --context environment=dev --force

logs:
    @echo "Tailing ingestion Lambda logs (Ctrl+C to stop)..."
    @aws logs describe-log-groups --log-group-name-prefix /aws/lambda/ --query 'logGroups[].logGroupName' --output text | grep ArubaIngestionFn | xargs -I{} aws logs tail {} --follow

test: install
    $(PY) -m pytest

local-run: install
    @if [ -z "$$LOCAL_DB_SECRET_FILE" ] || [ -z "$$LOCAL_API_SECRET_FILE" ] || [ -z "$$DB_HOST" ]; then \
      echo "Set LOCAL_DB_SECRET_FILE, LOCAL_API_SECRET_FILE, DB_HOST env vars."; exit 1; fi
    ENVIRONMENT=dev $(PY) lambda_py/ingestion_handler.py

format: install
    @which black >/dev/null 2>&1 || $(PIP) install black
    $(PY) -m black lambda_py tests

clean:
    rm -rf $(VENV) cdk.out
    find . -name '__pycache__' -prune -exec rm -rf {} +
```// filepath: /Users/mcolatosti/Library/CloudStorage/OneDrive-ExponentInc/Documents/coderepos/aruba/aruba-central-cdk-ingestion/Makefile
# Python-only Aruba Central CDK Ingestion

VENV = .venv
PY = $(VENV)/bin/python
PIP = $(VENV)/bin/pip
CDK ?= cdk   # Assumes AWS CDK CLI installed globally (npm i -g aws-cdk)

.DEFAULT_GOAL := help

.PHONY: help venv install synth diff deploy deploy-prod destroy clean logs test local-run format

help:
    @echo "Targets:"
    @echo "  venv        - Create virtual environment"
    @echo "  install     - Install Python deps (infra + runtime + tests)"
    @echo "  synth       - cdk synth"
    @echo "  diff        - cdk diff"
    @echo "  deploy      - Deploy (dev)"
    @echo "  deploy-prod - Deploy (prod)"
    @echo "  destroy     - Destroy current dev stack"
    @echo "  logs        - Tail Lambda logs"
    @echo "  test        - Run pytest"
    @echo "  local-run   - Run ingestion locally (needs env + secret JSON files)"
    @echo "  format      - Auto-format (requires 'black')"
    @echo "  clean       - Remove build artifacts and venv"

venv:
    @test -d $(VENV) || python3 -m venv $(VENV)
    @$(PIP) install --upgrade pip >/dev/null

install: venv
    $(PIP) install -r requirements.txt

synth: install
    $(CDK) synth

diff: install
    $(CDK) diff

deploy: install
    $(CDK) deploy --context environment=dev --require-approval never

deploy-prod: install
    $(CDK) deploy --context environment=prod

destroy: install
    $(CDK) destroy --context environment=dev --force

logs:
    @echo "Tailing ingestion Lambda logs (Ctrl+C to stop)..."
    @aws logs describe-log-groups --log-group-name-prefix /aws/lambda/ --query 'logGroups[].logGroupName' --output text | grep ArubaIngestionFn | xargs -I{} aws logs tail {} --follow

test: install
    $(PY) -m pytest

local-run: install
    @if [ -z "$$LOCAL_DB_SECRET_FILE" ] || [ -z "$$LOCAL_API_SECRET_FILE" ] || [ -z "$$DB_HOST" ]; then \
      echo "Set LOCAL_DB_SECRET_FILE, LOCAL_API_SECRET_FILE, DB_HOST env vars."; exit 1; fi
    ENVIRONMENT=dev $(PY) lambda_py/ingestion_handler.py

format: install
    @which black >/dev/null 2>&1 || $(PIP) install black
    $(PY) -m black lambda_py tests

clean:
    rm -rf $(VENV) cdk.out
    find . -name '__pycache__' -prune -exec rm -rf {} +