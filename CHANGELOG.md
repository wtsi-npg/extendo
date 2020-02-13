# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased] - [![Build Status](https://travis-ci.org/kjsanger/extendo.svg?branch=devel)](https://travis-ci.org/kjsanger/extendo)

### Added

### Changed

### Fixed

## [2.0.0] - 2020-02-13

### Added

- This changelog.
- Support to stop idle and long-running clients.
- IdleTime() and Runtime() to Client.
- Locking to Client for state updates and iRODS operations.

### Changed

- Modify NewClientPool to accept ClientPoolParams. This is an API breaking
  change as it modifies the signature of NewClientPool to allow a number of
  pool settings to be managed.
- Switch from ClientPool from using channels to mutex.

- Bump Miniconda3 from 4.5.11 to 4.6.14
- Bump baton from 2.0.0 to 2.0.1
- Bump github.com/rs/zerolog from 1.17.2 to 1.18.0
- Bump github.com/pkg/errors from 0.8.1 to 0.9.1
- Bump github.com/kjsanger/logshim from 1.0.0 to 1.1.0
- Bump github.com/kjsanger/logshim-zerolog from 1.0.0 to 1.1.0
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
