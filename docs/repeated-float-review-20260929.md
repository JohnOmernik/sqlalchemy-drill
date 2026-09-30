# Repeated FLOAT4/FLOAT8 review regression

## Boundary and release

Reviewed #13 head: `39b88b0839e7256904cb4154621602b47af84d2c`.
It was merged as `96ff31162426f91f0ea60d76c8b495ee935376b2` before the
changes-requested review. Version 1.1.11.6 was already published on 2026-09-29;
the corrected code therefore declares **1.1.11.7** rather than overwriting it.
The merge tree and reviewed head have the same package implementation.

## Fix

Drill labels repeated floating-point columns as `FLOAT4` or `FLOAT8`, just like
scalars. Convert list elements recursively; retain `None`; pass dictionaries
through unchanged so maps' own field types are not coerced. Scalar finite and
nonfinite floats retain their existing behavior. The changelog now limits the
float-conversion promise to Drill >=1.19, whose metadata precedes the rows.
A trailing-metadata unit test documents the older behavior without pretending
older Drill servers received a new type conversion.

## Fresh live proof

One loopback-only Apache Drill container, started with host load below 80:

```sh
docker run -d --name review-drill-0929 --cpus 4 --memory 4g \
  -p 127.0.0.1:28047:8047 --entrypoint /bin/bash \
  apache/drill@sha256:5e0a13d686633595c0b607454b24931c6ec44ef22910e523195fc62d53e04184 \
  -c '$DRILL_HOME/bin/drill-embedded -f <(sleep infinity)'
```

`SELECT version FROM sys.drillbits` reports **1.22.0**. The review's exact
query returns raw REST metadata `['FLOAT8']` and row `{'arr': [1.5, 2.5]}`:

```sql
SELECT convert_from('[1.5, 2.5]', 'JSON') arr FROM (VALUES(1))
```

Run the committed live suite against this server:

```sh
DRILL_REVIEW_URL='drill+sadrill://localhost:28047/dfs.tmp' \
  python -m pytest -q test/test_repeated_float_live.py
python -m pytest -q test/test_sqlalchemy2_reflection.py tools/test_check_dist.py
docker rm -f review-drill-0929
```

The live fixture exercises both `drill` and `drill+sadrill` URLs: JSON repeated
floats, a CTAS Parquet repeated-float column, and scalar DOUBLE/FLOAT,
NaN/Infinity/-Infinity/NULL/DECIMAL controls. CTAS tables use unique names and
are dropped in `finally`. CI also runs this file against its pinned Drill
1.21.2 testcontainer; without `DRILL_REVIEW_URL` it uses the existing fixture.

| Code / Python 3.11.15 | Unit + distribution checks | Live Drill 1.22.0 |
| --- | --- | --- |
| Reviewed implementation | 4 new array/map cases fail with TypeError | 4 fail / 2 pass |
| Fix / SQLAlchemy 2.0.52 | 424 pass | 6 pass |
| Fix / SQLAlchemy 1.4.54 | 423 pass / 1 version-specific skip | 6 pass |

Before the fix, all four JSON/Parquet array checks fail at `float(list)`;
the two scalar controls already pass. Afterward arrays contain Python floats.
The server-free cases also cover FLOAT4, empty/nested arrays, NULL elements,
nonfinite elements, maps, and maps inside arrays. Maps preserve identity when
passed directly to the typecaster. This scoped replay does not claim fresh
coverage of TLS, external plugins, JDBC or ODBC.
