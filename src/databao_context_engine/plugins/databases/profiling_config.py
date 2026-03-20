from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class ProfilingConfig(BaseModel):
    """Data profiling configuration.

    Attributes:
        enabled: master switch. If False, data profiling is disabled entirely.

    Future extensions:
        scope: include/exclude rules controlling which tables/columns get profiled
    """

    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    # TODO: Add scope field similar to SamplingConfig when column-level profiling filtering is needed
