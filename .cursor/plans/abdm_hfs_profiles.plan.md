---
name: ABDM HFS Profiles
overview: Implement full NDHM R4 IG profile validation on Helios FHIR Server with warn-on-write enforcement (choice 1.A + 2.B).
todos:
  - id: ig-pin-materialize
    content: "Pin NDHM R4 NPM; generate full profile-manifest.json via build_ig_profile_manifest"
    status: pending
  - id: validation-service
    content: "ProfileValidationService + AppState; HFS retains registry from HFS_PROFILE_MANIFEST"
    status: completed
  - id: validate-operation
    content: "POST /{type}/$validate and /{type}/{id}/$validate"
    status: completed
  - id: write-enforcement
    content: "warn mode on create/update/patch/batch; strict via HFS_PROFILE_VALIDATION_MODE"
    status: completed
  - id: hts-ndhm-terminology
    content: "Import NDHM CodeSystem/ValueSet into HTS from manifest lists"
    status: pending
  - id: ops-docs-tests
    content: "deploy env example, integration test with fixture manifest"
    status: completed
isProject: true
---

# ABDM profiles on Helios FHIR Server

## Locked choices

| Decision | Choice |
|----------|--------|
| IG scope | **Full NDHM R4 IG** (all StructureDefinitions from nrces.in NPM) |
| Write enforcement | **`warn`** — log issues, persist; **`strict`** later for 422 |

## Implementation status

Executing REST integration (Phases 3–5): [`crates/rest/src/profile_validation.rs`](crates/rest/src/profile_validation.rs), AppState, `$validate`, write hooks, HFS `build_hfs_app`.

See also prior plan detail in user Cursor plans `abdm_hfs_profiles_be706445`.
