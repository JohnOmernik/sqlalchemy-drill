## [1.1.11] - unreleased

### Fixed

- Compile schema-less tables without an empty `FROM` target or a leading dot.
- Render column references without a schema qualifier. Drill accepts only a
  one-part table qualifier, so every `SELECT` against a schema-qualified table
  previously failed with `VALIDATION ERROR: Table '<plugin>' not found`.
- Execute reflection statements through SQLAlchemy executable objects on
  SQLAlchemy 2, binding metadata values and quoting qualified Drill identifiers.
- Preserve bare-plugin/workspace lookup while binding literal metadata values
  and escaping LIKE wildcard characters instead of interpolating substring SQL.
- Escape REST DB-API qmark parameters once, ignoring question marks in SQL
  literals, identifiers and comments (including `/*/`), and never reinterpreting
  question marks introduced by parameter values.
- Preserve opaque DBAPI failures during dynamic column reflection and classpath
  existence probes: a failed SELECT is not proof that a table is absent.
- Define native `import_dbapi()` hooks directly on JDBC and ODBC dialects,
  retaining `dbapi()` compatibility aliases and avoiding SQLAlchemy's deprecated
  legacy-hook fallback. REST already provides its native hook.

### Changed

- `DrillIdentifierPreparer.format_drill_table()` now takes the schema and table
  name as separate arguments. The old `format_drill_table(path, isFile=...)`
  signature is no longer supported and raises `TypeError`. Use
  `format_drill_schema()` to format a schema on its own.
- `get_columns()` no longer interprets a `table_name` containing `SELECT ` as a
  subquery to reflect. Reflection now always treats the argument as an
  identifier. Callers that relied on passing a query must issue it directly.
- Non-file column reflection reads `INFORMATION_SCHEMA.COLUMNS` instead of
  `DESCRIBE`, and now reports a `nullable` flag.
- A `?` inside a quoted identifier is no longer substituted as a parameter, so
  ``cur.execute("... `a?b` ...", params)`` must no longer count it.
- The REST DB-API rejects parameters Drill cannot represent as literals
  (binary, non-finite floats and decimals, complex numbers, and time-zone-aware
  datetimes and times) instead of sending SQL that always fails, and rejects
  `str`/`bytes` as parameter sequences.
- `requires SQLAlchemy >= 1.4`, because reflection uses the 1.4 `Connection`
  execution API.

### Security and compatibility notes

- **The REST driver does not verify TLS certificates by default.**
  `verify_ssl` defaults to `False`, so `use_ssl=True` alone encrypts without
  authenticating the server. Pass `verify_ssl=True` for a trusted connection.
  This is long-standing behaviour and is unchanged here; changing the default
  is a separate breaking change.
- JDBC and ODBC still inherit `driver == "rest"` from the base dialect. That is
  wrong for both transports, but correcting public dialect metadata is a visible
  API change that deserves its own review rather than riding along with this
  fix. Nothing in this package dispatches on `driver`, and it is not a TLS
  control: JDBC and ODBC transport security is configured through their own
  connection strings.
- The JDBC and ODBC dialects are **not** covered by the automated tests here.
  Their `import_dbapi()` hooks are verified with stub modules; no live JDBC or
  ODBC server has been exercised. Treat those transports as unverified.
- Verified against Apache Drill 1.21.2 (pinned by image digest) over REST, and
  against SQLAlchemy 1.4.54, 2.0.52 and the 2.1 prerelease series. SQLAlchemy
  2.1 support is based on a prerelease and may change before its final release.
- No new Drill type mappings are claimed. In particular, `STRUCT` and `ARRAY`
  reflection remains `UserDefinedType` until round-trip behavior is verified;
  that placeholder cannot currently be rendered by the type compiler.


## [1.1.6] - 2025-02-24

### Fixed

- Parsing of empty result set data in sadrill.

### Changed

- Added a DB-API compliance test suite running against a local Drill using testcontainers.
-
## [1.1.5] - 2024-06-04

### Fixed

- Fix a leaked StopIteration from a generator in sadrill.

## [1.1.4] - 2023-10-23

### Fixed

- Add 'properties' as a reserved word.

## [1.1.3] - 2022-05-03

### Changed

- Fixed type casting bug which caused queries that returned null date or time
  values to raise an error in _drilldbapi.py.

## [1.1.2] - 2022-03-14

### Changed

- Add an impersonation_target parm to drill+sadrill URLs. When present,
  this parameter will be converted to a userName property in POSTs made to
  /query.json.

## [1.1.1] - 2021-07-28

### Fixed

- Backwards compatibility with Drill < 1.19, limited to returning all data
  values as strings. Users not able to upgrade to >= 1.19 must implement their
  own typecasting or use sqlalchemy-drill 0.3.

## [1.1.0] - 2021-07-21

**N.B.**: The drill+sadrill dialect in this release is not compatible with Drill
< 1.19.

### Changed

- Rewrite the drill+sadrill dialect using the ijson streaming parser.
