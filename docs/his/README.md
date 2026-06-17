# FHIR-Native Hospital Information System (HIS)

Architecture and implementation plan for building hospital operations on the Helios/Atrius FHIR stack.

## Documents

| Document | Description |
|----------|-------------|
| [fhir-native-his-plan.md](./fhir-native-his-plan.md) | Comprehensive phased plan — registration, scheduling, ADT, staffing, orders, integration |

## Related documentation

- [Clinical Reasoning & CDS Stack](../clinical-reasoning/README.md) — pathways, eCQM, JVM sidecar
- [Clinical Reasoning Forward Plan](../clinical-reasoning/forward-plan.md) — CDS roadmap (parallel track)
- [HFS Server Configuration](../../CLAUDE.md#hfs-server-configuration) — platform env vars and deployment

## Implementation phases (summary)

| Phase | Focus | Status |
|-------|-------|--------|
| 0 | Platform hardening + IG profiles + seed data | Pending |
| 1 | Patient registration & MPI | Pending |
| 2 | Scheduling & appointments | Pending |
| 3 | ADT (admit, transfer, discharge) | Pending |
| 4 | Staff rostering & duty assignment | Pending |
| 5 | Orders, CPOE & care coordination | Pending |
| 6 | External integrations & analytics | Pending |

See [fhir-native-his-plan.md](./fhir-native-his-plan.md) for full detail, architecture diagrams, and success criteria.
