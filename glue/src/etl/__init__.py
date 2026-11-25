"""Glue ETL package.

Split from the former single-file ``glue/etl_job.py`` (1,067 lines) into
focused modules; ``glue/etl_job.py`` is now only the thin driver that wires
the stages together.

Layout:

* ``config.py`` -- constants, Spark schemas, catalog DDL, argument parsing
* ``s3utils.py`` -- S3 control-plane helpers (list ``dt=`` prefixes, HEAD)
* ``silver.py`` -- bronze write + silver build/write
* ``gold.py`` -- dimensions (SCD2 / SCD1 / calendar) + fact
* ``catalog.py`` -- Glue Data Catalog registration + dimension loading
* ``quality.py`` -- data-quality gates
* ``retry.py`` -- exponential-backoff retry (mirrors
  ``src/pipeline_kit/retry.py``; this package is deployed as a
  self-contained ``--extra-py-files`` zip)
* ``spark.py`` -- Spark/Glue session bootstrap

Spark imports live only in the modules that need them (``silver``, ``gold``,
``catalog``, ``quality``, ``spark``). ``config`` and ``retry`` import
nothing from Spark, so they can be unit-tested anywhere.
"""

