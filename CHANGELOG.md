# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased] - [![Unit tests](https://github.com/wtsi-npg/extendo/actions/workflows/run-tests.yml/badge.svg)](https://github.com/wtsi-npg/extendo/actions/workflows/run-tests.yml)

## [2.6.0] - 2023-04-17

### Fixed

- Hang when baton crashed

### Changed

- Bump github.com/onsi/ginkgo/v2 from 2.1.6 to 2.9.1
- Bump github.com/onsi/gomega from 1.20.2 to 1.27.6
- Bump github.com/stretchr/testify from 1.8.0 to 1.8.2
- Bump github.com/rs/zerolog from 1.28.0 to 1.29.0

## [2.5.0] - 2022-10-13

### Added

- Checksum verification on put

Removal of a workaround that was created for iRODS 4.1.* now allows
extendo to support iRODS' checksum verification feature for `put` operations.
This feature calculates a checksum from the local file on disk and compares
this to the checksum of the remote data object in iRODS, as part of the `put`
operation. This is the same behaviour as the iRODS icommand `iput -K`.

### Removed

- iRODS 4.1.* PutDataObject workaround
- iRODS 4.2.10 GitHub Action tests

### Changed

- Build with Go 1.17
- Migrate GitHub Action from Ubuntu 18.04 to 20.04
- Update baton to 4.0.0 in GitHub Action tests

- Bump github.com/onsi/ginkgo from 1.16.1 to 2.1.6
- Bump github.com/onsi/gomega from 1.11.0 to 1.20.2
- Bump github.com/stretchr/testify from 1.7.0 to 1.8.0
- Bump github.com/rs/zerolog from 1.21.0 to 1.28.0


## [2.4.0] - 2021-04-15

### Added

- Github Actions test automation

### Changed

- Bump github.com/onsi/ginkgo from 1.15.0 to 1.16.1
- Bump github.com/onsi/gomega from 1.10.5 to 1.11.0
- Bump github.com/pkg/errors from 0.8.1 to 0.9.1
- Bump github.com/rs/zerolog from 1.19.0 to 1.21.0
- Bump github.com/wtsi-npg/logshim from 1.2.0 to 1.3.0
- Bump github.com/wtsi-npg/logshim-zerolog 1.2.0 to 1.3.0

## [2.3.0] - 2021-02-25

### Changed

- Migrated the repository from github.com/kjsanger to github.com/wtsi-npg

- Bump github.com/onsi/ginkgo from 1.12.2 to 1.15.0
- Bump github.com/onsi/gomega from 1.9.0 to 1.10.5
- Bump github.com/stretchr/testify from 1.6.1 to 1.7.0
- Bump github.com/wtsi-npg/logshim from 1.1.0 to 1.2.0
- Bump github.com/wtsi-npg/logshim-zerolog from 1.0.0 to 1.2.0

## [2.2.0] - 2020-06-11

### Added

- Added HasNamespace, Namespace, WithNamespace, WithoutNamespace
  methods to AVU.

### Changed

- Deprecated MakeAVU.

- Bump github.com/stretchr/testify from 1.5.1 to 1.6.1
- Bump github.com/onsi/ginkgo from 1.12.0 to 1.12.2
- Bump github.com/rs/zerolog from 1.18.0 to 1.19.0

### Fixed

## [2.1.0] - 2020-03-24

### Added

- HasMetadatum, HasSomeMetadata and HasAllMetadata methods to RodsItem.
- Parent method to Collection and DataObject.
- Test on Go 1.14

### Changed

- AddMetadata and RemoveMetadata are now idempotent.

- Bump github.com/stretchr/testify from 1.4.0 to 1.5.1

### Fixed

## [2.0.0] - 2020-02-13

### Added

- This changelog.
- Support to stop idle and long-running clients.
- IdleTime and Runtime methods to Client.
- Locking to Client for state updates and iRODS operations.

### Changed

- Modify NewClientPool to accept ClientPoolParams. This is an API breaking
  change as it modifies the signature of NewClientPool to allow a number of
  pool settings to be managed.
- Switch ClientPool from using channels to mutex.

- Bump Miniconda3 from 4.5.11 to 4.6.14
- Bump baton from 2.0.0 to 2.0.1
- Bump github.com/rs/zerolog from 1.17.2 to 1.18.0
- Bump github.com/pkg/errors from 0.8.1 to 0.9.1
- Bump github.com/wtsi-npg/logshim from 1.0.0 to 1.1.0
- Bump github.com/wtsi-npg/logshim-zerolog from 1.0.0 to 1.1.0
- Bump github.com/onsi/gomega from 1.7.1 to 1.9.0
- Bump github.com/onsi/ginkgo from 1.10.3 to 1.12.0

## [1.1.0] - 2019-12-05

### Changed

- Refactor sorting to use sort.SliceStable.

- Bump github.com/onsi/gomega from 1.5.0 to 1.7.1
- Bump github.com/onsi/ginkgo from 1.8.0 to 1.10.3
- Bump github.com/rs/zerolog from 1.14.3 to 1.17.2
- Bump github.com/stretchr/testify from 1.3.0 to 1.4.0

### Fixed

- Improvements to documentation, formatting and logging.

## [1.0.0] - 2019-10-14

### Added

- ClientPool to help applications in client lifecyle management.
- Collection and DataObject API.
- ListChecksum operation.
- ListItem operation.
- Travis CI configuration.
- List, Put, Chmod, Remove, Metamod, Metaquery operations.
