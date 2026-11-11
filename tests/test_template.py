"""Invariants the SAM template must keep. Parsed, not deployed."""

from __future__ import annotations

import pathlib

import pytest

yaml = pytest.importorskip("yaml")

TEMPLATE = pathlib.Path(__file__).resolve().parents[1] / "template.yaml"


class _Cfn(yaml.SafeLoader):
    pass


def _tag(loader, suffix, node):
    if isinstance(node, yaml.ScalarNode):
        return {suffix: loader.construct_scalar(node)}
    if isinstance(node, yaml.SequenceNode):
        return {suffix: loader.construct_sequence(node)}
    return {suffix: loader.construct_mapping(node)}


_Cfn.add_multi_constructor("!", _tag)


@pytest.fixture(scope="module")
def t() -> dict:
    # _Cfn extends SafeLoader; the only extra constructor builds plain dicts for CFN tags.
    return yaml.load(TEMPLATE.read_text(), Loader=_Cfn)  # cqa: ignore=PY-SEC-001 reason="SafeLoader subclass"


def test_queue_visibility_is_six_times_worker_timeout(t):
    worker_timeout = t["Resources"]["WorkerFunction"]["Properties"]["Timeout"]
    visibility = t["Resources"]["OrdersQueue"]["Properties"]["VisibilityTimeout"]
    assert visibility >= 6 * worker_timeout


def test_slowest_batch_fits_the_worker_timeout(t):
    max_delay_ms = t["Parameters"]["DependencyDelayMs"]["MaxValue"]
    batch = t["Resources"]["WorkerFunction"]["Properties"]["Events"]["Orders"]["Properties"]["BatchSize"]
    worker_timeout = t["Resources"]["WorkerFunction"]["Properties"]["Timeout"]
    assert batch * max_delay_ms / 1000 < worker_timeout


def test_unsent_index_is_keys_only(t):
    [gsi] = t["Resources"]["OrdersTable"]["Properties"]["GlobalSecondaryIndexes"]
    assert gsi["IndexName"] == "gsi1"
    assert gsi["Projection"]["ProjectionType"] == "KEYS_ONLY"


def test_queue_age_and_dlq_alarms_exist(t):
    alarms = {
        k: v["Properties"]["MetricName"] for k, v in t["Resources"].items() if v["Type"] == "AWS::CloudWatch::Alarm"
    }
    assert alarms["QueueAgeAlarm"] == "ApproximateAgeOfOldestMessage"
    assert alarms["DlqNotEmptyAlarm"] == "ApproximateNumberOfMessagesVisible"


def test_api_requires_iam_auth(t):
    assert t["Resources"]["OrdersApi"]["Properties"]["Auth"]["DefaultAuthorizer"] == "AWS_IAM"


def test_worker_has_partial_batch_failures_and_a_concurrency_cap(t):
    props = t["Resources"]["WorkerFunction"]["Properties"]["Events"]["Orders"]["Properties"]
    assert props["FunctionResponseTypes"] == ["ReportBatchItemFailures"]
    assert "MaximumConcurrency" in props["ScalingConfig"]
