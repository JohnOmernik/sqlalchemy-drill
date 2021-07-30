## [Unreleased]

## [1.1.1] - 2021-07-28

### Fixed

- Backwards compatibility with Drill < 1.19, limited to returning
  all data values as strings. Users not able to upgrade to >= 1.19
  must implement their own typecasting or use sqlalchemy-drill 0.3.

## [1.1.0] - 2021-07-21

**N.B.**: The drill+sadrill dialect in this release is not compatible with Drill
< 1.19.

### Changed

- Rewrite the drill+sadrill dialect using the ijson streaming parser.
