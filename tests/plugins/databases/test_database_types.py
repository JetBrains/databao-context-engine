from databao_context_engine.plugins.databases.databases_types import (
    CardinalityBucket,
    CardinalityRange,
)


def test_cardinality_bucket_range_is_defined_for_every_bucket() -> None:
    for bucket in CardinalityBucket:
        if bucket is CardinalityBucket.UNKNOWN:
            assert bucket.range is None
        else:
            assert isinstance(bucket.range, CardinalityRange)
