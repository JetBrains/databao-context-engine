# Project Notes — September 2025

## Overview

This project integrates database introspection, dbt metadata, and document embeddings to create a unified data context
engine.

## Goals

- Enable users to search and reason about their data environment in natural language.
- Extend support to plain text and PDF documents to capture unstructured knowledge.
- Provide consistent output structure under the `/output/run-<timestamp>` folders.

## Next Steps

1. Finish the file ingestion subsystem (loader, service, exporter).
2. Add PDF parsing and chunking support.
3. Implement caching for large document embeddings.
4. Evaluate chunking strategies for better recall during querying.

## Open Questions

- Should we embed entire paragraphs or split by sentence boundaries?
- How do we handle private/sensitive data inside document chunks?
- Could the system use source control metadata (e.g., Git history) to enrich document context?

## Summary

Once completed, the system will allow querying across databases, dbt models, and raw files — a single interface for
exploring data knowledge.