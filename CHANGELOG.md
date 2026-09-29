## [1.1.11.6] - unreleased

### Fixed

- REST results decoded DOUBLE and FLOAT columns as `Decimal`, and NaN,
  Infinity and -Infinity as the strings `'NaN'`, `'Infinity'` and
  `'-Infinity'`, although the cursor description reports FLOAT. FLOAT4 and
  FLOAT8 values are now Python floats (or `None`).
- Reflection of DECIMAL columns in file-backed tables (for example Parquet)
  returned `UserDefinedType`, because Drill reports them as
  `VARDECIMAL(p, s)`. They now reflect as `DECIMAL(p, s)`, and the REST
  cursor description carries the precision and scale.

## [1.1.11.5] - unreleased

### Fixed

- Reflection of dynamic-schema plugins such as Kafka returned only the
  `**` placeholder column that INFORMATION_SCHEMA publishes for them. When
  that placeholder is the only column, the real columns are now read from a
  `SELECT * ... LIMIT 1` probe, as for file-backed tables.
- A missing MongoDB collection raised the probe's `DatabaseError` from
  `has_table()` and autoload. Absence is now proven, like missing files, by a
  missing-object diagnostic plus a fresh, complete, unlimited and nonempty
  `INFORMATION_SCHEMA.TABLES` listing of the database that lacks the name;
  empty, limited or failed listings still preserve the original error.
- An HTTPS login whose certificate fails verification now raises
  `TransportError` explaining that certificates are verified by default since
  1.1.11.4 and naming the fix (`verify_ssl=<path to the CA bundle>`), instead
  of a bare SSL error. Verification is never disabled automatically.
- Query-profile reads for guarded absence used a fixed 30 s timeout per
  request (up to seven requests plus about 5 s of sleeps). The whole poll now
  shares one budget: the connection's `request_timeout`, else 30 s.
- The cancellation tag was per cursor, so `Cursor.cancel()` could reach a
  different statement from the same cursor. Every `execute()` now uses a
  fresh tag; `cancel()` only reaches the cursor's latest statement and returns
  `False` before the first `execute()`.
- `get_view_definition()` without a schema and without a database in the URL
  bound `TABLE_SCHEMA = NULL` and never matched. It now searches all schemas
  and requires a unique match (`InvalidRequestError` if the view name exists
  in several schemas).
- When `request_timeout` expires, the driver now cancels the timed-out
  statement on the server (found by its tag) instead of leaving it running.

### Added

- `Cursor.cancel_group` (optional, 32 lowercase hex characters) marks every
  statement the cursor runs with a shared ID in addition to its own tag, and
  `Connection.cancel_query_group(id)` cancels whichever of them is running.
  This suits callers that must choose a cancel ID before execution starts,
  such as a SQL editor's "Stop" button, possibly from another connection.

## [1.1.11.4] - unreleased

### Breaking change

- **HTTPS connections now verify the server certificate by default.** With
  `use_ssl=true` and no `verify_ssl`, 1.1.11.3 and earlier encrypted without
  checking the certificate; 1.1.11.4 checks it against the system trust store.
  A server with a self-signed or private-CA certificate that previously
  connected now fails verification. Migrate by setting
  `verify_ssl=<path to the CA bundle that signed the server certificate>` in
  the connection URL (or the `connect()` argument). `verify_ssl=false` restores
  the old unverified behaviour and is not recommended.

### Fixed

- REST DATE, TIME and TIMESTAMP values equal to zero epoch milliseconds
  (1970-01-01, midnight, 1970-01-01 00:00:00) were returned as `None`. Only a
  JSON null now decodes to `None`.
- REST TIME and TIMESTAMP values keep their millisecond fraction instead of
  being truncated to whole seconds.

- A `verify_ssl=true` / `verify_ssl=false` URL value was passed to requests as
  the string, which requests reads as a CA bundle path, so `verify_ssl=true`
  failed with "Could not find a suitable TLS CA certificate bundle". Boolean
  spellings now become booleans; any other value is still a CA bundle path.
  (Correction: an earlier version of this note said the default was
  unchanged. It is not; see "Breaking change" below.)

- A provably absent file-backed table could surface as the opaque
  `DatabaseError` instead of `False` / `NoSuchTableError` on a busy server:
  the failed query's profile was read for only about 0.3 s before its error
  was published. The profile is now polled with backoff for about 5 s. The
  opaque REST error text is still never treated as evidence, because
  permission and other failures produce the same text.
- `Cursor.get_query_id()` raised `AttributeError`; it now returns the query
  ID once Drill has sent it, else `None`.

- TLS certificates are now verified by default when `use_ssl` is set
  (system trust store). `verify_ssl=<CA bundle path>` selects a CA bundle and
  `verify_ssl=false` explicitly disables verification (logged as a warning).
  Previously an HTTPS connection without `verify_ssl` accepted any certificate.
- A streamed query no longer reads its whole response body into memory:
  a debug log statement evaluated `Response.text` for every query, so
  `stream_results` had no effect and memory grew with the result size. The
  body is also parsed in 64 KiB chunks instead of one byte at a time.
- REST transport failures (connection errors, resets, timeouts, including
  mid-stream) raise DB-API `TransportError` (an `OperationalError`) instead of
  raw `requests` exceptions, and the dialect reports them and closed
  connections as disconnects. `pool_pre_ping` and SQLAlchemy invalidation now
  replace a pooled connection whose HTTP session failed or was closed, instead
  of handing it out and failing the first statement.
- `Cursor.close()` works after its connection was closed or invalidated.

### Added

- Opt-in `request_timeout=<seconds>` connection option (URL query parameter)
  applied to every REST request; a timeout raises `OperationalError`. Without
  it nothing changes: there is no client-side limit, as before.
- `Cursor.cancel()` cancels the statement the cursor is running, including
  before Drill has sent any result (for example a long aggregation). Every
  cursor statement carries a leading `/* sqlalchemy-drill:<tag> */` comment
  with a per-cursor tag (`Cursor.query_tag`); `cancel()` finds that unique tag
  in Drill's running-query list and calls `/profiles/cancel/{queryId}`.
  `Connection.cancel_query(query_id)` and `Connection.cancel_tagged_query(tag)`
  are also available, e.g. to cancel from another connection.
- `get_view_definition()` returns the stored view SQL from
  `INFORMATION_SCHEMA.VIEWS` (bound schema and view name) instead of raising
  `NotImplementedError`; an unknown view raises `NoSuchTableError`.

## [1.1.11.3] - unreleased

### Fixed

- Classify absent REST tables with verbose errors enabled as well as with the
  default non-verbose setting. When `errorMessage` cannot prove a missing-object
  `VALIDATION ERROR`, consult the query profile even if that message is nonempty:
  verbose Drill responses omit the authoritative error-class prefix.
- Keep the strict class check and fresh, complete, readable nonempty directory
  corroboration. Permission, syntax, transport and unprovable failures still
  raise; successful queries never fetch profiles. Exercise both REST error
  settings against Apache Drill 1.21.2 and reset the option after testing.

## [1.1.11.2] - unreleased

### Fixed

- REST file reflection reports absence only when proven: a missing-object
  `VALIDATION ERROR` must be corroborated by a fresh, complete, nonempty
  `SHOW FILES` listing that does not contain the requested name (or view).
  Traverse nested paths through readable ancestors; reject limited listings
  and paths whose glob/URI semantics cannot be checked by literal comparison.
  The nonempty listing witnesses readability under the actual filesystem
  identity, without guessing that identity from session users or file owners.
  Proven absence returns `False` from `has_table()` and raises `NoSuchTableError`
  from `get_columns()` / autoload.
- Permission denial and unproven absence preserve the original query error.
  In particular, empty listings, classpath resources without listings, unknown
  schemas, and failed or malformed corroboration never turn an error into
  absence. File existence probes read the table rather than accepting a
  directory entry as proof of access.
- Keep failure-path profile classification via `/profiles/{queryId}.json` when
  default REST responses omit `errorMessage`, using the query's authenticated
  session and briefly retrying late-published profiles. Syntax, transport,
  other error classes and unavailable or unclassifiable profiles still raise.
- Verify against Drill 1.21.2 that both `SHOW FILES` and
  `INFORMATION_SCHEMA.FILES` expose directory, permission, owner and group
  metadata. Those bits alone do not establish effective access: session user
  and file owner can both differ from the drillbit filesystem user, and these
  listings do not supply its group membership or effective ACLs. Empty listings
  remain unproven. Live regressions assert that a typo in a populated readable
  directory is absent, while an existing file beneath a `chmod 000` directory
  raises through `has_table()`, column reflection and autoload.

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
  is a separate breaking change. (Superseded: 1.1.11.4 verifies by default.)
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
