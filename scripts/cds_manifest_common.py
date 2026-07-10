"""Shared helpers for CDS Hooks manifest generation from KR PlanDefinitions."""

from __future__ import annotations

import base64
import json
import re
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

DEFAULT_KR_BASE_URL = "http://127.0.0.1:8079"

POPULATION_EXPRESSION_PRIORITY = (
    "Initial Population",
    "Numerator",
    "Denominator",
    "Denominator Exclusions",
)

# Standard patient-chart prefetch (reduces sidecar clinical round-trips).
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
    """Actionable message when KR has no PlanDefinitions."""
    return (
        "KR has no PlanDefinition resources.\n"
        "Import Atrius-authored artifacts, then regenerate the catalog:\n"
        "  IMPORT_ATRIUS=1 ./scripts/setup-plandefinition-cds-catalog.sh\n"
        "  # or: (cd $ATRIUS_IG && ./scripts/translate-cql.sh && \\\n"
        f"  #        ./scripts/import-atrius-kr-libraries.py --clinical-reasoning "
        f"--kr-base-url {kr_base})"
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


def slugify(value: str, max_len: int = 80) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip()).strip("-").lower()
    return slug[:max_len] or "service"


def human_title(library_id: str, expression: str) -> str:
    if expression in POPULATION_EXPRESSION_PRIORITY:
        return f"{library_id} — {expression}"
    return library_id


def plan_definition_data_requirements(plan: dict[str, Any]) -> list[dict[str, Any]]:
    """Prefetch DataRequirements live on ``action.input`` in R4 CDSHooksServicePlanDefinition."""
    requirements: list[dict[str, Any]] = []
    for action in plan.get("action") or []:
        requirements.extend(action.get("input") or [])
    # Backward compat for resources uploaded before the fix.
    if not requirements:
        requirements.extend(plan.get("dataRequirement") or [])
    return requirements


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
    for key, template in STANDARD_PATIENT_CHART_PREFETCH.items():
        prefetch.setdefault(key, template)
    return prefetch
