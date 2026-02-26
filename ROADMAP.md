# Helios FHIR Server — Roadmap

>
> This document outlines the development direction for the Helios FHIR Server. It is organized into three horizons — **Now**, **Next**, and **Later** — to set expectations without overpromising timelines. Items may shift between horizons as priorities evolve based on community feedback, production needs, and contributor availability.
>
> Want to influence the roadmap? Join our [weekly developer meeting](#community) or comment on a [GitHub Discussion](https://github.com/HeliosSoftware/hfs/discussions).

---

## ✅ Shipped

These capabilities are available today in the current release.

**Helios FHIR Server**

- [FHIR REST API server](crates/hfs/README.md) with CRUD operations, search, history, and batch/transaction support

**Persistence**
- [SQLite as a primary store](crates/persistence/README.md#sqlite-default)
- [SQLite as a primary store with Elasticsearch as a query secondary](crates/persistence/README.md#sqlite--elasticsearch)
- [PostgreSQL as a primary store](crates/persistence/README.md#postgresql)
- [PostgreSQL as a primary store with Elasticsearch as a query secondary](crates/persistence/README.md#postgresql--elasticsearch)

**Analytics & Tooling**
- [SQL on FHIR](crates/sof/README.md) — CLI and HTTP server
- [FHIRPath expression engine](crates/fhirpath/README.md) — CLI and HTTP server
- [Python bindings (pysof)](crates/pysof/README.md)

---

## 🔨 Now — Actively In Progress

Work that is currently underway or planned for the near term.

| Area | Item | Status |
|------|------|--------|
| **Compliance** | Audit logging (AuditEvent resource support) | 🔵 Design |
| **Standards** | FHIR Validation engine | 🔵 Design |
| **Standards** | Authentication & Authorization | 🔵 Design |
| **Documentation** | Project documentation website | 🔵 Design |
| **Persistence** | MongoDB as a primary store | 🟡 In progress |
| **Persistence** | S3 as a primary store | 🟡 In progress |

### Discussion Documents

We are actively developing community discussion documents on the following topics to gather feedback before implementation begins. These will be published as GitHub Discussions:

- **Authentication & Authorization** — Scoping the approach to identity, access control, and SMART on FHIR integration
- **Terminology** — Defining how code systems, value sets, and concept maps will be managed
- **Validation** — Establishing the strategy for StructureDefinition-based validation and profiles
- **Audit** — Designing the audit trail architecture and AuditEvent generation

---

## 🗺️ Next — Up After Current Work

These items are well-understood and will be picked up once current work completes.

### FHIR Server Capabilities
- **Bulk Data API** — Import and export (`$export` / `$import` operations)
- **FHIR Subscriptions** — Topic-based notification support
- **Terminology Server** — CodeSystem `$lookup`, ValueSet `$expand`, ConceptMap `$translate`
- **SMART on FHIR** — Full launch framework and scoped access
- **SQL on FHIR** — [SQL on FHIR operations](https://sql-on-fhir.org/ig/latest/operations.html) - using read-only database connections 

### Persistence Backends
- Cassandra as a primary store
- ClickHouse as a primary store
- S3 with Elasticsearch as a query secondary
- Cassandra with Elasticsearch as a query secondary

### Developer Experience
- **Administrative UI** — Web-based management console for server configuration and monitoring
- **MCP Server for FHIR API** — Model Context Protocol integration for the FHIR REST API
- **MCP Server for SQL on FHIR** — Model Context Protocol integration for analytics workflows
- **Deployment Cookbooks** — Reference architectures and deployment templates covering standalone development servers, single-node production setups, composite storage configurations, and full CQRS architectures on AWS, Azure, and GCP

---

## 🔭 Later — On the Horizon

Longer-term ideas we are exploring. These are not yet committed and may evolve significantly based on community input.

### Advanced Persistence
- Neo4j as a primary store
- PostgreSQL with Neo4j as a graph query secondary

### Persistence Advisor
An intelligent recommendation engine for storage configuration:
- Analyze a FHIR query and recommend an optimal persistence configuration
- Leverage historical benchmark data to inform recommendations
- Web UI for interactive configuration guidance

---

## Status Legend

| Icon | Meaning |
|------|---------|
| 🟡 | In progress — actively being developed |
| 🔵 | Design — in planning or community discussion phase |

---

## Community

We welcome contributors and feedback at every level — from opening issues to joining design discussions.

- **📋 GitHub Discussions:** [github.com/HeliosSoftware/hfs/discussions](https://github.com/HeliosSoftware/hfs/discussions)
- **🐛 Issues:** [github.com/HeliosSoftware/hfs/issues](https://github.com/HeliosSoftware/hfs/issues)
- **🗓️ Weekly Developer Meeting:** — Open to all. We review roadmap progress, discuss design decisions, and plan upcoming work. Details and updates are posted to [this GitHub Discussion](https://github.com/HeliosSoftware/hfs/discussions/40).

### How to Get Involved

1. **Comment on a discussion document** — Help shape the design of upcoming features
2. **Claim a roadmap item** — Open an issue or comment on an existing one to signal interest
3. **Join the weekly call** — Introduce yourself and find out where help is needed
4. **Contribute code** — See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines

---

*This roadmap is a living document. It does not represent a commitment or guarantee to deliver any feature by any particular date. Items may be reprioritized based on community needs, production feedback, and resource availability.*
