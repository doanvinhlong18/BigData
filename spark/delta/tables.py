"""Minimal Delta Lake Python wrapper for Spark jobs.

This project only needs a small subset of the `delta-spark` API:
`DeltaTable.forPath(...).alias(...).merge(...).whenMatchedUpdateAll()`
` .whenNotMatchedInsertAll().execute()`.

The wrapper delegates to the JVM Delta Lake classes that are provided by the
Delta JARs baked into the Spark image.
"""

from __future__ import annotations


class DeltaTable:
    def __init__(self, jdt):
        self._jdt = jdt

    @classmethod
    def forPath(cls, spark, path):
        jdt = spark._jvm.io.delta.tables.DeltaTable.forPath(spark._jsparkSession, path)
        return cls(jdt)

    @classmethod
    def isDeltaTable(cls, spark, path):
        return spark._jvm.io.delta.tables.DeltaTable.isDeltaTable(
            spark._jsparkSession, path
        )

    def alias(self, alias_name):
        return DeltaTable(self._jdt.alias(alias_name))

    def merge(self, source, condition):
        if hasattr(source, "_jdf"):
            source = source._jdf
        return _DeltaMergeBuilder(self._jdt.merge(source, condition))


class _DeltaMergeBuilder:
    def __init__(self, jbuilder):
        self._jbuilder = jbuilder

    def whenMatchedUpdateAll(self):
        return _DeltaMergeBuilder(self._jbuilder.whenMatchedUpdateAll())

    def whenNotMatchedInsertAll(self):
        return _DeltaMergeBuilder(self._jbuilder.whenNotMatchedInsertAll())

    def execute(self):
        return self._jbuilder.execute()

