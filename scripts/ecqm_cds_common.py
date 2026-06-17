"""Shared helpers for eCQM CDS Hooks manifest and PlanDefinition generation."""

from __future__ import annotations

import base64
import json
import re
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

DEFAULT_KR_BASE_URL = "http://127.0.0.1:8079"

# eCQM QICore Content IG — Library + Measure only (no PlanDefinition in the NPM package).
ECQM_NPM_PACKAGE_URL = (
    "https://build.fhir.org/ig/cqframework/ecqm-content-qicore-2025/package.tgz"
)

DEFAULT_SKIP_LIBRARY_IDS = frozenset(
    {
        "FHIRHelpers",
        "QICoreCommon",
        "QICore-ModelInfo",
        "Status",
        "SupplementalDataElements",
        "CQMCommon",
        "CumulativeMedicationDuration",
        "HospitalHarm",
        "Antibiotic",
        "AlaraCommonFunctions",
    }
)

MANIFEST_LIBRARY_PREFIX = "Manifest-"

DEFINE_RE = re.compile(
    r"^define\s+(?:\"([^\"]+)\"|([A-Za-z][A-Za-z0-9_]*))\s*:",
    re.MULTILINE,
)

POPULATION_EXPRESSION_PRIORITY = (
    "Initial Population",
    "Numerator",
    "Denominator",
    "Denominator Exclusions",
)

HELLO_WORLD_EXPRESSION_PRIORITY = (
    "PatientName",
    "HelloWorld",
    "Initial Population",
)

ECQM_POPULATION_EXPRESSIONS = (
    "Initial Population",
    "Denominator",
    "Numerator",
    "Denominator Exclusions",
)

CDS_HOOKS_SERVICE_PROFILE = (
    "http://hl7.org/fhir/StructureDefinition/cdshooksserviceplandefinition"
)
CQF_CDS_HOOKS_ENDPOINT = (
    "http://hl7.org/fhir/StructureDefinition/cqf-cdsHooksEndpoint"
)
PLAN_DEFINITION_TYPE_ECA = {
    "coding": [
        {
            "system": "http://terminology.hl7.org/CodeSystem/plan-definition-type",
            "code": "eca-rule",
            "display": "ECA Rule",
        }
    ]
}


def kr_get(base: str, path: str, tenant: str = "default") -> dict[str, Any]:
    url = f"{base.rstrip('/')}/{path.lstrip('/')}"
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/fhir+json",
            "X-Tenant-ID": tenant,
        },
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.load(resp)


def kr_put_resource(
    base: str,
    resource: dict[str, Any],
    tenant: str = "default",
) -> dict[str, Any]:
    resource_type = resource["resourceType"]
    resource_id = resource.get("id")
    if not resource_id:
        raise ValueError(f"{resource_type} missing id")
    url = f"{base.rstrip('/')}/{resource_type}/{resource_id}"
    body = json.dumps(resource).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="PUT",
        headers={
            "Content-Type": "application/fhir+json",
            "Accept": "application/fhir+json",
            "X-Tenant-ID": tenant,
        },
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        payload = resp.read()
    return json.loads(payload) if payload else {}


def kr_put_binary(
    base: str,
    binary_id: str,
    manifest_bytes: bytes,
    tenant: str = "default",
) -> None:
    payload = {
        "resourceType": "Binary",
        "id": binary_id,
        "contentType": "application/json",
        "data": base64.b64encode(manifest_bytes).decode("ascii"),
    }
    url = f"{base.rstrip('/')}/Binary/{binary_id}"
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="PUT",
        headers={
            "Content-Type": "application/fhir+json",
            "Accept": "application/fhir+json",
            "X-Tenant-ID": tenant,
        },
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        if resp.status not in (200, 201):
            raise RuntimeError(f"Binary PUT unexpected status {resp.status}")


def kr_resource_count(
    base: str,
    resource_type: str,
    tenant: str = "default",
) -> int:
    """Return total resources of ``resource_type`` on KR (FHIR ``_summary=count``)."""
    bundle = kr_get(base, f"{resource_type}?_summary=count", tenant)
    total = bundle.get("total")
    if total is None:
        return len(fetch_paged_resources(base, resource_type, tenant))
    return int(total)


def missing_plandefinitions_help(*, kr_base: str) -> str:
    """Actionable message when KR has no PlanDefinitions (expected for eCQM NPM)."""
    return (
        "The eCQM NPM package ships Library + Measure only — no PlanDefinition.\n"
        "Synthesize CDSHooksServicePlanDefinition resources from those libraries, "
        "then upload to KR:\n"
        f"  python3 scripts/generate-ecqm-plandefinitions.py --kr-base-url {kr_base} --upload\n"
        "Or from the package tarball (Libraries must still be on KR for $apply):\n"
        "  python3 scripts/generate-ecqm-plandefinitions.py --download --upload "
        f"--kr-base-url {kr_base}\n"
        "One-shot (import libs if needed + synthesize + manifest):\n"
        "  ECQM_IMPORT_LIBS=1 ./scripts/setup-plandefinition-cds-catalog.sh"
    )


def fetch_paged_resources(
    base: str,
    resource_type: str,
    tenant: str = "default",
) -> list[dict[str, Any]]:
    resources: list[dict[str, Any]] = []
    offset = 0
    page_size = 200
    while True:
        qs = urllib.parse.urlencode({"_count": page_size, "_offset": offset})
        bundle = kr_get(base, f"{resource_type}?{qs}", tenant)
        entries = bundle.get("entry") or []
        for entry in entries:
            res = entry.get("resource")
            if res and res.get("resourceType") == resource_type:
                resources.append(res)
        if len(entries) < page_size:
            break
        offset += page_size
    resources.sort(key=lambda r: r.get("id", ""))
    return resources


def decode_cql(library: dict[str, Any]) -> str | None:
    for part in library.get("content") or []:
        if part.get("contentType") == "text/cql" and part.get("data"):
            raw = base64.b64decode(part["data"])
            return raw.decode("utf-8", errors="replace")
    return None


def expression_names_from_cql(cql: str) -> list[str]:
    names: list[str] = []
    for match in DEFINE_RE.finditer(cql):
        line_start = cql.rfind("\n", 0, match.start()) + 1
        line = cql[line_start : match.end()]
        if "define function" in line:
            continue
        name = match.group(1) or match.group(2)
        if name:
            names.append(name)
    return names


def slugify(value: str, max_len: int = 80) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip()).strip("-").lower()
    return slug[:max_len] or "service"


def pascal_name(*parts: str, max_len: int = 254) -> str:
    out = ""
    for part in parts:
        for token in re.split(r"[^A-Za-z0-9]+", part):
            if token:
                out += token[0].upper() + token[1:]
    if not out:
        out = "PlanDefinition"
    if not re.match(r"^[A-Z]", out):
        out = "A" + out
    return out[:max_len]


def pick_expressions(
    library_id: str,
    defines: list[str],
    populations_mode: bool,
    measure_expressions: list[str] | None = None,
) -> list[str]:
    if measure_expressions:
        if populations_mode:
            return [e for e in ECQM_POPULATION_EXPRESSIONS if e in measure_expressions]
        for candidate in POPULATION_EXPRESSION_PRIORITY:
            if candidate in measure_expressions:
                return [candidate]
        return measure_expressions[:1]

    if populations_mode:
        return [e for e in ECQM_POPULATION_EXPRESSIONS if e in defines]

    priority = (
        HELLO_WORLD_EXPRESSION_PRIORITY
        if library_id == "HelloWorld"
        else POPULATION_EXPRESSION_PRIORITY
    )
    for candidate in priority:
        if candidate in defines:
            return [candidate]
    return [defines[0]] if defines else []


def human_title(library_id: str, expression: str) -> str:
    if expression in POPULATION_EXPRESSION_PRIORITY:
        return f"{library_id} — {expression}"
    return library_id


def library_reference(library: dict[str, Any]) -> str:
    library_id = library["id"]
    version = library.get("version")
    if library.get("url") and version:
        return f"{library['url']}|{version}"
    return f"Library/{library_id}"


def measure_reference(measure: dict[str, Any]) -> str:
    """Absolute canonical URL for RelatedArtifact.resource (not a relative ref)."""
    version = measure.get("version")
    if measure.get("url"):
        if version:
            return f"{measure['url']}|{version}"
        return str(measure["url"])
    measure_id = measure.get("id") or "unknown"
    return f"https://madie.cms.gov/Measure/{measure_id}"


def canonical_tail(url: str) -> str:
    return url.rstrip("/").rsplit("/", 1)[-1]


def measure_library_keys(measure: dict[str, Any]) -> list[str]:
    keys: list[str] = []
    for ref in measure.get("library") or []:
        if isinstance(ref, str):
            keys.append(ref)
            keys.append(canonical_tail(ref))
    measure_id = measure.get("id")
    if measure_id:
        keys.append(measure_id)
    return keys


def population_expressions_from_measure(measure: dict[str, Any]) -> list[str]:
    expressions: list[str] = []
    seen: set[str] = set()
    for group in measure.get("group") or []:
        for population in group.get("population") or []:
            expr = (population.get("criteria") or {}).get("expression")
            if isinstance(expr, str) and expr.strip() and expr not in seen:
                seen.add(expr)
                expressions.append(expr.strip())
    return expressions


def build_measure_index(
    measures: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for measure in measures:
        for key in measure_library_keys(measure):
            index.setdefault(key, measure)
    return index


def default_data_requirements() -> list[dict[str, Any]]:
    return [
        {
            "type": "Patient",
            "profile": [
                "http://hl7.org/fhir/us/qicore/StructureDefinition/qicore-patient"
            ],
        }
    ]


def plan_definition_data_requirements(plan: dict[str, Any]) -> list[dict[str, Any]]:
    """Prefetch DataRequirements live on ``action.input`` in R4 CDSHooksServicePlanDefinition."""
    requirements: list[dict[str, Any]] = []
    for action in plan.get("action") or []:
        requirements.extend(action.get("input") or [])
    # Backward compat for resources uploaded before the fix.
    if not requirements:
        requirements.extend(plan.get("dataRequirement") or [])
    return requirements


# Standard patient-chart prefetch for eCQM / Atrius CDS (reduces sidecar clinical round-trips).
# Covers the most common QI-Core / AtriusIn retrieve types across CMS and companion libraries.
STANDARD_PATIENT_CHART_PREFETCH: dict[str, str] = {
    "patient": "Patient/{{context.patientId}}",
    "conditions": "Condition?patient={{context.patientId}}",
    "encounters": "Encounter?patient={{context.patientId}}",
    "observations": "Observation?patient={{context.patientId}}",
    "procedures": "Procedure?patient={{context.patientId}}",
    "medicationRequests": "MedicationRequest?patient={{context.patientId}}",
    "immunizations": "Immunization?patient={{context.patientId}}",
    "diagnosticReports": "DiagnosticReport?patient={{context.patientId}}",
    "serviceRequests": "ServiceRequest?patient={{context.patientId}}",
    "allergies": "AllergyIntolerance?patient={{context.patientId}}",
    "coverage": "Coverage?beneficiary=Patient/{{context.patientId}}",
}


def data_requirements_to_prefetch(
    data_requirements: list[dict[str, Any]] | None,
) -> dict[str, str]:
    prefetch: dict[str, str] = {}
    for index, req in enumerate(data_requirements or []):
        resource_type = req.get("type") or "Resource"
        key = slugify(resource_type) if index == 0 else slugify(f"{resource_type}-{index}")
        if resource_type == "Patient":
            prefetch[key] = "Patient/{{context.patientId}}"
        else:
            prefetch[key] = f"{resource_type}?patient={{context.patientId}}"
    if not prefetch:
        return dict(STANDARD_PATIENT_CHART_PREFETCH)
    # Ensure common chart types are present for measures that retrieve them.
    for key, template in STANDARD_PATIENT_CHART_PREFETCH.items():
        prefetch.setdefault(key, template)
    return prefetch
