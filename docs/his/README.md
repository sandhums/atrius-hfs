# FHIR-Native Hospital Information System (HIS)

Architecture and implementation plan for building hospital operations on the Helios/Atrius FHIR stack.

## Documents

| Document | Description |
|----------|-------------|
| [fhir-native-his-plan.md](./fhir-native-his-plan.md) | Comprehensive phased plan — registration, scheduling, ADT, staffing, orders, integration |
| [atrius-his repo](../../../atrius-his) | **Layer 2 domain services** — platform hardening, `his-server`, future registration/scheduling/ADT crates |

> **Implementation:** HIS domain services live in the sibling [`atrius-his`](../../../atrius-his) repository. Platform hardening (Phase 0) is underway there.

## Related documentation

- [Clinical Reasoning & CDS Stack](../clinical-reasoning/README.md) — pathways, eCQM, JVM sidecar
- [Clinical Reasoning Forward Plan](../clinical-reasoning/forward-plan.md) — CDS roadmap (parallel track)
- [HFS Server Configuration](../../CLAUDE.md#hfs-server-configuration) — platform env vars and deployment

## Implementation phases (summary)

| Phase | Focus | Status |
|-------|-------|--------|
| 0 | Platform hardening + IG profiles + seed data | ✓ atrius-his |
| 1 | Patient registration & MPI | ✓ smoke |
| 2 | Scheduling & appointments (base R4) | ✓ smoke |
| 3 | ADT (admit, transfer, discharge) | ✓ smoke |
| **3.5** | **Scheduling/ADT hardening — Schedule+Slot+Appointment profiles, iCalendar RRULE, start-visit, `$validate`** | **Active** |
| 4 | Staff rostering & duty assignment | Pending |
| 5a | Consultation notes (Composition on Encounter) | Pending |
| 5b | Lab orders & CPOE (ServiceRequest + Task) | Pending |
| 6 | External integrations & analytics | Pending |

See [fhir-native-his-plan.md](./fhir-native-his-plan.md) for full detail, architecture diagrams, and success criteria.
