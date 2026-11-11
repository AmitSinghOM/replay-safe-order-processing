STACK ?= replay-safe-orders

.PHONY: test lint build deploy delete replay-dry-run

test:
	uv run --python 3.12 pytest

lint:
	uv run --python 3.12 ruff check src tests scripts

build:
	sam validate --lint && sam build

deploy: build
	sam deploy --stack-name $(STACK) --resolve-s3 --capabilities CAPABILITY_IAM --no-confirm-changeset

delete:
	sam delete --stack-name $(STACK) --no-prompts

replay-dry-run:
	uv run --python 3.12 --extra aws python scripts/replay_dlq.py --stack $(STACK) --operator "$(OPERATOR)" --reason "$(REASON)"
