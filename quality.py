"""
Reusable data-quality helpers for the medallion pipeline.

Usage from a Databricks notebook (Repos checkout):
    %run ../src/quality

Or package as a wheel (`python -m build`) and install it on the cluster,
then `from lakehouse.quality import ...`.

All helpers are pure PySpark — no Databricks-only APIs — so they can be
unit-tested locally with `pip install pyspark`.
"""

from typing import Callable, Dict, List, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataQualityError(Exception):
    """Raised when a blocking data-quality check fails."""


def check_not_null(df: DataFrame, cols: Sequence[str]) -> Dict[str, int]:
    """Count nulls per column. Returns {col: null_count}."""
    counts = df.select(
        *[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]
    ).collect()[0]
    return {c: counts[c] for c in cols}


def check_unique(df: DataFrame, cols: Sequence[str]) -> int:
    """Return the number of duplicate rows on the given key columns."""
    total = df.count()
    distinct = df.dropDuplicates(list(cols)).count()
    return total - distinct


def check_range(df: DataFrame, col: str, min_val=None, max_val=None) -> int:
    """Return the number of rows where `col` falls outside [min_val, max_val]."""
    predicate = F.lit(False)
    if min_val is not None:
        predicate = predicate | (F.col(col) < F.lit(min_val))
    if max_val is not None:
        predicate = predicate | (F.col(col) > F.lit(max_val))
    return df.filter(predicate).count()


def quarantine_invalid(
    df: DataFrame, valid_predicate, quarantine_table: str
) -> DataFrame:
    """
    Split `df` into clean and bad rows using `valid_predicate` (a Column).
    Bad rows are appended as Delta to `quarantine_table` (a fully-qualified
    table name, e.g. "catalog.schema.claims_quarantine") for later inspection;
    the clean DataFrame is returned.
    """
    bad = df.filter(~valid_predicate)
    bad_count = bad.count()
    if bad_count > 0:
        (
            bad.withColumn("_quarantined_at", F.current_timestamp())
            .write.format("delta")
            .mode("append")
            .saveAsTable(quarantine_table)
        )
        print(f"Quarantined {bad_count} invalid rows -> {quarantine_table}")
    return df.filter(valid_predicate)


def run_checks(
    df: DataFrame,
    checks: List[Dict],
    fail_fast: bool = True,
) -> Dict[str, object]:
    """
    Run a list of check dicts, e.g.:
        {"name": "claim_id not null", "fn": lambda d: check_not_null(d, ["claim_id"])}

    Each "fn" must return 0/{} when the check passes, or a non-zero /
    non-empty-dict-of-failures when it fails. Returns a results dict and
    raises DataQualityError on the first failure when fail_fast=True.
    """
    results: Dict[str, object] = {}
    for check in checks:
        outcome = check["fn"](df)
        failed = bool(outcome) if not isinstance(outcome, dict) else any(
            v != 0 for v in outcome.values()
        )
        results[check["name"]] = {"passed": not failed, "detail": outcome}
        status = "PASS" if not failed else "FAIL"
        print(f"[{status}] {check['name']}: {outcome}")
        if failed and fail_fast:
            raise DataQualityError(f"Data quality check failed: {check['name']} -> {outcome}")
    return results
