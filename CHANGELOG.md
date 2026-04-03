# Changelog

Notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.1] - 2026-04-03

### Added

- Added `get_columns` helper for getting column names from both lazy and eager frames
- `PolarsFrame` type that includes both pl.LazyFrame and pl.DataFrame
- Decorators for `@requires_eager` (convert lazy -> eager -> return lazy) and `@requires_lazy` to simplify conversions

## [0.2.0] - 2026-04-01

### Added

- Tests for `cast` and `utils`.
- Lazy/eager operations

### Changed

- `polars_readstat` engine no longer accepts 'engine' as an argument (defaults to rust)

## [0.1.0] - 2026-03-13

### Added

- Initial implementation.


[unreleased]: https://github.com/kpwhri/arctickit/compare/v0.2.1...HEAD

[0.2.1]: https://github.com/kpwhri/arctickit/compare/v0.2.0...v0.2.1

[0.2.0]: https://github.com/kpwhri/arctickit/compare/v0.1.0...v0.2.0

[0.1.0]: https://github.com/kpwhri/arctickit/releases/tag/v0.1.0