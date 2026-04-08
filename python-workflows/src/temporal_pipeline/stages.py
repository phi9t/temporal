"""User-facing stages API — called inside @workflow.run methods.

Usage inside a pipeline workflow::

    from temporal_pipeline import stages

    prep = await stages.run("data_prep", engine="spark", inputs={...})
    ckpt = prep.artifact("packed_dataset")
"""

from __future__ import annotations

import re
from typing import Any

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporal_pipeline.models import StageHandle, StageSpec
    from temporal_pipeline.stage_workflow import StageWorkflow

_VALID_NAME = re.compile(r"^[a-zA-Z0-9_-]+$")


def _validate_name(name: str) -> None:
    if not name:
        raise ValueError("Stage name must not be empty")
    if not _VALID_NAME.match(name):
        raise ValueError(
            f"Stage name '{name}' contains invalid characters. "
            "Use only alphanumeric, hyphens, and underscores."
        )


def _validate_engine(engine: str) -> None:
    if not engine:
        raise ValueError("Engine name must not be empty")


async def run(
    name: str,
    engine: str,
    inputs: dict[str, Any] | None = None,
    config: dict[str, Any] | None = None,
    timeout_seconds: int = 86400,
) -> StageHandle:
    """Start a child StageWorkflow and wait for it to complete.

    Returns a :class:`StageHandle` whose ``.artifact(name)`` method gives
    typed artifact refs from the committed manifest.
    """
    _validate_name(name)
    _validate_engine(engine)
    if timeout_seconds <= 0:
        raise ValueError(f"timeout_seconds must be positive, got {timeout_seconds}")

    spec = StageSpec(
        name=name,
        engine=engine,
        inputs=inputs if inputs is not None else {},
        config=config if config is not None else {},
        timeout_seconds=timeout_seconds,
    )
    parent_wf_id = workflow.info().workflow_id
    child_id = f"{parent_wf_id}-stage-{name}"

    handle: StageHandle = await workflow.execute_child_workflow(
        StageWorkflow.run,
        spec,
        id=child_id,
    )
    return handle


async def run_parallel(
    specs: list[dict[str, Any]],
) -> list[StageHandle]:
    """Run multiple stages concurrently and collect all results.

    Each element in *specs* is a dict with keys matching :func:`run`'s params:
    ``name``, ``engine``, ``inputs``, ``config``, ``timeout_seconds``.

    Raises ValueError if any spec is missing required keys.
    """
    if not specs:
        return []

    parent_wf_id = workflow.info().workflow_id
    handles = []
    for i, s in enumerate(specs):
        if not isinstance(s, dict):
            raise TypeError(f"specs[{i}] must be a dict, got {type(s).__name__}")
        for key in ("name", "engine"):
            if key not in s:
                raise ValueError(f"specs[{i}] missing required key '{key}'")

        name = s["name"]
        engine = s["engine"]
        _validate_name(name)
        _validate_engine(engine)

        spec = StageSpec(
            name=name,
            engine=engine,
            inputs=s.get("inputs") or {},
            config=s.get("config") or {},
            timeout_seconds=s.get("timeout_seconds", 86400),
        )
        child_id = f"{parent_wf_id}-stage-{spec.name}"
        h = await workflow.start_child_workflow(
            StageWorkflow.run,
            spec,
            id=child_id,
        )
        handles.append(h)
    return [await h for h in handles]
