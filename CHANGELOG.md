# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `TransferCodec::DispositionView` exposes the fields of a modified outcome: `delivery_failed`, `undeliverable_here` and `message_annotations`

### Fixed

- `Codec.skip_value` skips `short` (0x61) values instead of raising, which rejected any message containing one

## [0.1.0] - 2026-09-28

### Added

- AMQP 1.0 type system (`Value`, `Codec`) with depth, size and count limits when decoding untrusted input
- Frame reading and writing (`FrameReader`, `FrameWriter`)
- Performatives: `open`, `begin`, `attach`, `flow`, `detach`, `end`, `close` with `source`, `target` and `error`, plus SASL frame constants
- Allocation-free `transfer`, `disposition` and `flow` codecs (`TransferCodec`)
- Streaming readers for decoding message sections without allocating
- Extracted from [LavinMQ](https://github.com/cloudamqp/lavinmq)
