## [Unreleased]

## [1.1.5] - 2024-06-04

### Fixed

- Fix a leaked StopIteration from a generator.

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
