"""Data models for the Temporal pipeline orchestration SDK."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class StageState(str, enum.Enum):
    CREATED = "CREATED"
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    COMMITTING_OUTPUTS = "COMMITTING_OUTPUTS"
    SUCCEEDED = "SUCCEEDED"
    FAILED_TRANSIENT = "FAILED_TRANSIENT"
    BLOCKED_ON_BUG = "BLOCKED_ON_BUG"
    PAUSED_FOR_DECISION = "PAUSED_FOR_DECISION"


class RepairAction(str, enum.Enum):
    RESUME_FROM_BOUNDARY = "RESUME_FROM_BOUNDARY"
    RETRY_STAGE = "RETRY_STAGE"
    SKIP_STAGE = "SKIP_STAGE"
    ABORT_PIPELINE = "ABORT_PIPELINE"


# ---------------------------------------------------------------------------
# Artifact references
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ArtifactRef:
    """Typed reference to a committed artifact (URI, not payload)."""

    name: str
    uri: str
    artifact_type: str = ""
    metadata: dict[str, str] = field(default_factory=dict)
    stage_name: str = ""
    run_id: str = ""

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("ArtifactRef.name must not be empty")
        if not self.uri:
            raise ValueError("ArtifactRef.uri must not be empty")


@dataclass
class ArtifactManifest:
    """Collection of artifact refs produced by one stage execution."""

    stage_name: str
    run_id: str
    artifacts: dict[str, ArtifactRef] = field(default_factory=dict)
    committed_at: str = ""

    def __post_init__(self) -> None:
        if not self.stage_name:
            raise ValueError("ArtifactManifest.stage_name must not be empty")


# ---------------------------------------------------------------------------
# External job references
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ExternalJobRef:
    """Opaque reference to a job running in an external system."""

    engine: str
    job_id: str
    metadata: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.engine:
            raise ValueError("ExternalJobRef.engine must not be empty")
        if not self.job_id:
            raise ValueError("ExternalJobRef.job_id must not be empty")


@dataclass(frozen=True)
class ExternalJobState:
    """Normalized status snapshot from an external job system."""

    state: StageState
    message: str = ""
    progress_pct: float = 0.0
    last_heartbeat: str = ""


# ---------------------------------------------------------------------------
# Stage specification
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StageSpec:
    """Everything needed to submit a stage to an external backend."""

    name: str
    engine: str
    inputs: dict[str, Any] = field(default_factory=dict)
    config: dict[str, Any] = field(default_factory=dict)
    timeout_seconds: int = 86400

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("StageSpec.name must not be empty")
        if not self.engine:
            raise ValueError("StageSpec.engine must not be empty")
        if self.timeout_seconds <= 0:
            raise ValueError(
                f"StageSpec.timeout_seconds must be positive, got {self.timeout_seconds}"
            )


# ---------------------------------------------------------------------------
# Handles
# ---------------------------------------------------------------------------


@dataclass
class StageHandle:
    """Durable reference to a completed (or in-progress) stage execution."""

    stage_name: str
    workflow_id: str
    run_id: str
    manifest: ArtifactManifest | None = None

    def artifact(self, name: str) -> ArtifactRef:
        """Return a specific artifact ref from this stage's committed manifest."""
        if not name:
            raise ValueError("Artifact name must not be empty")
        if self.manifest is None:
            raise ValueError(
                f"Stage '{self.stage_name}' has no committed manifest yet"
            )
        if name not in self.manifest.artifacts:
            raise KeyError(
                f"Artifact '{name}' not found in stage '{self.stage_name}'. "
                f"Available: {list(self.manifest.artifacts.keys())}"
            )
        return self.manifest.artifacts[name]


@dataclass
class SubworkflowHandle:
    """Reference to a completed macro-stage (group of stages)."""

    boundary_name: str
    workflow_id: str
    run_id: str
    manifests: list[ArtifactManifest] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Bug tracking and repair
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BugRecord:
    """Recorded failure / bug info for a stage."""

    stage_name: str
    error_type: str
    error_message: str
    timestamp: str = ""
    debug_bundle_uri: str = ""


@dataclass(frozen=True)
class RepairPlan:
    """Durable recovery action to apply after a bug."""

    action: RepairAction
    target_stage: str = ""
    resume_from_boundary: str = ""
    patch: dict[str, Any] = field(default_factory=dict)
    preserve_artifacts: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Pipeline-level models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PipelineReq:
    """Top-level request to run a pipeline."""

    pipeline_name: str
    inputs: dict[str, Any] = field(default_factory=dict)
    config: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.pipeline_name:
            raise ValueError("PipelineReq.pipeline_name must not be empty")


@dataclass
class FinalArtifacts:
    """Terminal output of a pipeline workflow."""

    artifacts: dict[str, ArtifactRef] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Internal: stage workflow carry-over state for Continue-As-New
# ---------------------------------------------------------------------------


@dataclass
class StageCarryOver:
    """State carried across Continue-As-New boundaries in StageWorkflow."""

    spec: StageSpec
    state: StageState = StageState.CREATED
    job_ref: ExternalJobRef | None = None
    manifest: ArtifactManifest | None = None
    bug: BugRecord | None = None
    event_count: int = 0
