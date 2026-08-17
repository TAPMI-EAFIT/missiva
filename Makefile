.DEFAULT_GOAL := help

.PHONY: help format check mypy example-pub example-sub example-pubsub example-request-reply example-duplex

VENV := $(CURDIR)/.venv
# Same-line env: make starts a new shell per recipe. PYTHONPATH=src is required
# because Python 3.14 skips ``_*.pth`` (uv's editable hook for this package).
run-example = VIRTUAL_ENV="$(VENV)" PATH="$(VENV)/bin:$$PATH" PYTHONPATH="$(CURDIR)/src" python

help: ## Print available targets, what they run, and whether they rewrite files
	@echo "Missiva — local quality checks and examples"
	@echo
	@echo "Usage: make <target>"
	@echo
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-24s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

format: ## Rewrite Python files in place with ruff format (whole repo)
	uv run ruff format .

check: ## Lint the repo with ruff check; reports issues, does not modify files
	uv run ruff check .

mypy: ## Strict type-check of src and tests (mypy + Pydantic plugin)
	uv run mypy

example-pub: ## publish() on a reused session (examples/pub_once.py)
	$(run-example) examples/pub_once.py

example-sub: ## listen() until cancelled (examples/sub_loop.py)
	$(run-example) examples/sub_loop.py

example-pubsub: ## listen() + publish() transform (examples/pubsub.py)
	$(run-example) examples/pubsub.py

example-request-reply: ## request(): command then one ACK (examples/request_reply.py)
	$(run-example) examples/request_reply.py

example-duplex: ## listen() and publish() on the same session (examples/duplex.py)
	$(run-example) examples/duplex.py
