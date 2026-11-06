# Search parameter naming quirks (`partof` vs `part-of`)

Operational reference for the FHIR spec's inconsistent `partOf` search-parameter
spellings, HFS's lenient parameter handling, and the Atrius alias set that smooths
it over. Last verified 2026-10-30 against clinical HFS (8082).

## The spec quirk (frozen across R4/R4B/R5/R6)

FHIR's naming convention (kebab-case of the element name) says these should all be
`part-of`, but three resources predate the convention and are grandfathered — the
spec can never rename them without breaking deployed clients:

| Spelling | Resources |
|---|---|
| `part-of` (majority, 8) | CarePlan, Communication, Encounter, MedicationStatement, Observation, Procedure, QuestionnaireResponse, Task |
| `partof` (outliers, 3) | Location, Organization, ResearchStudy\* |

\* ResearchStudy keeps `partof` in R4/R4B; R5/R6 drop the parameter entirely.
Location and Organization keep `partof` in every version.

Confirm any time from the downloaded spec bundles in `data/`:

```bash
python3 -c "
import json
d = json.load(open('data/search-parameters-r4.json'))
for e in d['entry']:
    r = e['resource']
    if r.get('code') in ('partof','part-of'):
        print(r['code'], r.get('base'), r.get('expression'))"
```

## HFS behavior: unknown params are silently dropped (lenient default)

Per-request `Prefer` handling, implemented in `crates/rest/src/handlers/search.rs`:

- **Default (lenient)**: an unknown/misnamed search parameter is **dropped from the
  filter** — the response is `200` with *unfiltered* results, the param omitted from
  the self link, and an OperationOutcome note attached to the Bundle. An empty
  result therefore does not prove "no matches"; it may mean "param ignored."
- **`Prefer: handling=strict`**: unknown params fail with an OperationOutcome
  `invalid` error. Send this on searches in dev/test so misnamed params fail loudly.

Example of the trap (before the aliases below): `Location?part-of=X` (hyphenated,
not the spec spelling) returned **all** Locations with `200`.

## The Atrius alias set

HFS supports site-defined SearchParameter resources
(`crates/fhir/src/search/loader.rs` — custom files in the data directory register
for all tenants at boot; stored `SearchParameter` resources register per tenant at
write time via `tenant_registries` invalidation).

`data/atrius-search-params.json` ships Atrius aliases so the **majority spelling
works everywhere**:

- `part-of` on Location → `Location.partOf`
- `part-of` on Organization → `Organization.partOf`
- `part-of` on ResearchStudy → `ResearchStudy.partOf`

Both spellings are indexed and advertised in the CapabilityStatement; the spec's
`partof` keeps working unchanged. **Inside the Atrius stack, always use `part-of`.**
Portable clients talking to arbitrary FHIR servers must still handle `partof`.

(The same file also carries `Location physical-type` and ChargeItem/ChargeItemDefinition
custom params — it is the canonical place for Atrius search extensions.)

## Recipe: adding a custom search parameter

1. Add the SearchParameter resource to `data/atrius-search-params.json` (Bundle
   entry; see existing entries for shape). Boot registration covers all tenants.
2. For immediate effect on a running server (per tenant, no restart):

   ```bash
   curl -X PUT "$HFS/SearchParameter/{id}" \
     -H "Authorization: Bearer $TOKEN" -H "X-Tenant-ID: $TENANT" \
     -H "Content-Type: application/fhir+json" -d @param.json
   ```

3. Backfill index rows for already-stored resources (new writes index
   automatically):

   ```bash
   curl -X POST "$HFS/{ResourceType}/\$reindex" \
     -H "Authorization: Bearer $TOKEN" -H "X-Tenant-ID: $TENANT"
   curl "$HFS/\$reindex-status/{jobId}" -H ...   # poll until status=completed
   ```

4. Verify: the param appears in `GET /metadata`, a strict-mode query is accepted,
   and a known-good value returns the expected rows under both spellings.

## Related

- `docs/his/fhir-native-his-plan.md` — bed board note (client-side filter born
  from this quirk; can move to server-side `Location?part-of=`).
- Search conformance work: `docs/search-conformance-plan.md`,
  `docs/search-modifier-spec-compliance-plan.md`.
