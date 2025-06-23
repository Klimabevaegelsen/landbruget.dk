# Google Drive Data Pipeline Documentation

Welcome to the Google Drive Data Pipeline documentation. This guide will help you understand, install, and use the pipeline effectively.

## Getting Started

- [Setup Guide](setup.md) - Instructions for installing and configuring the pipeline
- [User Guide](user_guide.md) - How to use the pipeline, including command-line options

## Understanding the Pipeline

- [Architecture](architecture.md) - Technical architecture and component design
- [Data Flow](architecture.md#data-flow) - How data moves through the pipeline
- [Interfaces](architecture.md#key-interfaces) - Key programming interfaces

## Reference

- [Command-Line Options](user_guide.md#command-line-options) - Available CLI options
- [Configuration](setup.md#configuration) - Environment variables and config files
- [Data Models](architecture.md#data-models) - Core data structures
- [Directory Structure](architecture.md#directory-structure) - Codebase organization

## Troubleshooting

- [Troubleshooting Guide](troubleshooting.md) - Solutions to common problems
- [Error Recovery](troubleshooting.md#recovery-strategies) - How to recover from failures

## Development

- [Adding New Transformers](architecture.md#extensibility) - Extending for new file types
- [Test Suite](../tests/README.md) - Understanding and running tests

## Examples

- [Example Workflows](user_guide.md#common-workflows) - Common usage patterns
- [Configuration Examples](user_guide.md#example-configuration-file) - Sample config files

---

This pipeline follows medallion architecture with Bronze and Silver layers to extract, transform, and load data from Google Drive sources. It's designed to be robust, extensible, and maintainable for long-term data pipeline operations. 