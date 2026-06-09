# Request for Comments (RFCs)

This directory contains records of all major architectural and design decisions for the ETL Data Pipeline.

## Why RFCs?
We use RFCs to provide a living history of the "why" behind our architecture. They allow for collaborative discussion on GitHub before any major code is written.

## How to Propose a New RFC
1.  Copy the [RFC Template](./0000-template.md) to a new file named `XXXX-brief-title.md`.
2.  Fill out the draft and open a Pull Request.
3.  Add the new entry to the table below with a "Draft" status.
4.  Once consensus is reached, update the status to "Accepted" and merge the PR.

## RFC Index

| Number | Title | Status | Date | TL;DR |
| :--- | :--- | :--- | :--- | :--- |
| 0001 | [Pipeline Core Architecture](./0001-pipeline-core-architecture.md) | Draft | 2026-06-09 | Go interfaces, lifecycle contracts, in-process transport, and autonomous supervision for multi-pipeline/multi-sink ETL with at-least-once delivery |
| 0000 | [Template](./0000-template.md) | - | - | Base template for new RFCs |
