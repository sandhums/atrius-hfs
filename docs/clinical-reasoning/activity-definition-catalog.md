# ActivityDefinition catalog

Authoring reference for **reusable clinical actions** (labs, imaging, meds, referrals, tasks) lives in **AtriusIGDraft**:

**[AtriusIGDraft/docs/activity-definition-catalog-authoring.md](../../../AtriusIGDraft/docs/activity-definition-catalog-authoring.md)**

## Stack integration

| Layer | Role |
|-------|------|
| **AtriusIGDraft `catalogs/`** | FSH-authored ActivityDefinitions on KR |
| **PlanDefinition** | `definitionCanonical` → catalog AD |
| **cr-fhir-bridge** | `POST /ActivityDefinition/{id}/$apply` (FHIR Parameters) |
| **JVM sidecar** | CQF `ActivityDefinitionProcessor` |
| **Clinical HFS** | CRUD — POST accepted proposals (no operational tables required) |
| **clinical UI + BFF** | RequestGroup proposals → chart write |

## Smoke test

After KR import:

```bash
./scripts/activity-definition-apply-smoke.sh atrius-ad-lab-troponin-cardiac
./scripts/activity-definition-apply-smoke.sh atrius-ad-referral-cardiology
```

## ER chest pain

The ACS order set references catalog ADs (ECG, troponin, aspirin, cardiology consult). Pathway-specific ADs remain for STEMI cath lab, troponin guidance, and non-ACS branch stubs.

See [forward-plan.md](./forward-plan.md) Phase C.
