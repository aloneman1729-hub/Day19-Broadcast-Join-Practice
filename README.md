# Day 19 — Spark Broadcast Join Practice

A hands-on Apache Spark project demonstrating Broadcast Join and comparing it with a normal Sort Merge Join.

## Project Objective

This project demonstrates how Apache Spark can efficiently join a large fact dataset with a small reference dataset by broadcasting the smaller dataset to executors.

The application demonstrates:

- Normal Join
- Broadcast Join
- Physical execution plans
- Transaction analysis
- Branch and region analysis

## Project Architecture

```text
1,000,000 Transactions
        |
        | branch_id
        v
   JOIN Operation
        ^
        | branch_id
        |
4 Branch Master Records
        |
        v
Enriched Transaction Data
        |
        +---- Region Analysis
        |
        +---- Branch Analysis
