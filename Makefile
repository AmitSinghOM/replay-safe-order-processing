STACK ?= replay-safe-orders
LT_STACK ?= replay-safe-orders-lt
PY = uv run --python 3.12
export PYTHONPATH := src:.

.PHONY: test test-fast lint build deploy delete replay-dry-run \
	loadtest-local loadtest-deploy loadtest-aws loadtest-delete

test:
	$(PY) pytest

test-fast:
	$(PY) pytest -m "not slow"

lint:
	$(PY) ruff check src tests scripts loadtest && $(PY) ruff format --check src tests scripts loadtest

build:
	sam validate --lint && sam build

deploy: build
	sam deploy --stack-name $(STACK) --resolve-s3 --capabilities CAPABILITY_IAM --no-confirm-changeset \
		--tags project=replay-safe-orders

delete:
	sam delete --stack-name $(STACK) --no-prompts

replay-dry-run:
	$(PY) --extra aws python scripts/replay_dlq.py --stack $(STACK) --operator "$(OPERATOR)" --reason "$(REASON)"

# ---- failure 4: slow dependency ---------------------------------------------

loadtest-local:
	$(PY) python -m loadtest.local_sim --out results/local

# Separate stack, tagged purpose=loadtest; aws_run refuses any other stack.
loadtest-deploy: build
	sam deploy --stack-name $(LT_STACK) --resolve-s3 --capabilities CAPABILITY_IAM --no-confirm-changeset \
		--tags project=replay-safe-orders purpose=loadtest \
		--parameter-overrides DependencyDelayMs=$(or $(DELAY_MS),500) WorkerMaxConcurrency=$(or $(CONCURRENCY),2)

loadtest-aws:
	$(PY) --extra aws python -m loadtest.aws_run --stack $(LT_STACK) $(ARGS)

loadtest-delete:
	sam delete --stack-name $(LT_STACK) --no-prompts
