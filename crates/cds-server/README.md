# cds-server

Axum [`Router`](https://docs.rs/axum) for [CDS Hooks](https://cds-hooks.hl7.org/): `GET /cds-services`, `POST /cds-services/{id}`, `POST /cds-services/{id}/feedback`.

Depends on [`helios-cds-hooks`](../cds-hooks) for protocol types and `CdsHooksService`. This crate adds an object-safe `CdsServiceDispatch` and `cds_hooks_router` for wiring.

**Run the demo binary** (uses [`cds-core`](../cds-core) for evaluation, not inline logic)

```bash
cargo run -p cds-server --features binary -- --port 8088
```

**Library usage**

```rust
use std::sync::Arc;
use cds_server::{CdsServiceDispatch, CdsServiceRegistry, ServiceWrapper, cds_hooks_router};
// Register `Arc<dyn CdsServiceDispatch>` from `ServiceWrapper::new(Arc::new(YourService))`, then `cds_hooks_router(registry)`.
```

Put rules and [FHIR Clinical Reasoning](https://www.hl7.org/fhir/clinicalreasoning-module.html) in **`cds-core`** (or your app), not here.
