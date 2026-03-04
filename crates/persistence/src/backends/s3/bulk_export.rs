//! Bulk export implementation for the S3 backend.
//!
//! Implements `BulkExportStorage` and `ExportDataProvider`. Export jobs are
//! persisted as a small JSON state object in S3 and run synchronously within
//! the `start_export` call, writing NDJSON output parts directly to S3.

use std::collections::BTreeSet;

use async_trait::async_trait;
use chrono::Utc;

use crate::core::bulk_export::{
    BulkExportStorage, ExportDataProvider, ExportJobId, ExportManifest, ExportOutputFile,
    ExportProgress, ExportRequest, ExportStatus, NdjsonBatch, TypeExportProgress,
};
use crate::error::{BulkExportError, StorageError, StorageResult};
use crate::tenant::TenantContext;

use super::backend::{S3Backend, TenantLocation};
use super::models::ExportJobState;

#[async_trait]
impl BulkExportStorage for S3Backend {
    async fn start_export(
        &self,
        tenant: &TenantContext,
        request: ExportRequest,
    ) -> StorageResult<ExportJobId> {
        if request.output_format != "application/fhir+ndjson" {
            return Err(StorageError::BulkExport(
                BulkExportError::UnsupportedFormat {
                    format: request.output_format,
                },
            ));
        }

        let active_exports = self.list_exports(tenant, false).await?;
        if active_exports.len() >= 5 {
            return Err(StorageError::BulkExport(
                BulkExportError::TooManyConcurrentExports { max_concurrent: 5 },
            ));
        }

        let job_id = ExportJobId::new();
        let progress = ExportProgress::accepted(job_id.clone(), request.level.clone(), Utc::now());
        let state = ExportJobState {
            request,
            progress,
            manifest: None,
        };

        self.save_export_state(tenant, &job_id, &state).await?;

        if let Err(err) = self.run_export_job(tenant, &job_id).await {
            let _ = self
                .mark_export_failed(tenant, &job_id, &err.to_string())
                .await;
        }

        Ok(job_id)
    }

    async fn get_export_status(
        &self,
        tenant: &TenantContext,
        job_id: &ExportJobId,
    ) -> StorageResult<ExportProgress> {
        Ok(self.load_export_state(tenant, job_id).await?.progress)
    }

    async fn cancel_export(
        &self,
        tenant: &TenantContext,
        job_id: &ExportJobId,
    ) -> StorageResult<()> {
        let mut state = self.load_export_state(tenant, job_id).await?;

        if state.progress.status.is_terminal() {
            return Err(StorageError::BulkExport(BulkExportError::InvalidJobState {
                job_id: job_id.to_string(),
                expected: "accepted or in-progress".to_string(),
                actual: state.progress.status.to_string(),
            }));
        }

        state.progress.status = ExportStatus::Cancelled;
        state.progress.completed_at = Some(Utc::now());
        state.progress.error_message = None;
        state.progress.current_type = None;

        self.save_export_state(tenant, job_id, &state).await
    }

    async fn delete_export(
        &self,
        tenant: &TenantContext,
        job_id: &ExportJobId,
    ) -> StorageResult<()> {
        let location = self.tenant_location(tenant)?;

        if !self.export_job_exists(&location, job_id).await? {
            return Err(StorageError::BulkExport(BulkExportError::JobNotFound {
                job_id: job_id.to_string(),
            }));
        }

        let prefix = location.keyspace.export_job_prefix(job_id.as_str());
        for object in self.list_objects_all(&location.bucket, &prefix).await? {
            self.delete_object(&location.bucket, &object.key).await?;
        }

        Ok(())
    }

    async fn get_export_manifest(
        &self,
        tenant: &TenantContext,
        job_id: &ExportJobId,
    ) -> StorageResult<ExportManifest> {
        let state = self.load_export_state(tenant, job_id).await?;

        if state.progress.status != ExportStatus::Complete {
            return Err(StorageError::BulkExport(BulkExportError::InvalidJobState {
                job_id: job_id.to_string(),
                expected: "complete".to_string(),
                actual: state.progress.status.to_string(),
            }));
        }

        if let Some(manifest) = state.manifest {
            return Ok(manifest);
        }

        let location = self.tenant_location(tenant)?;
        let manifest_key = location.keyspace.export_job_manifest_key(job_id.as_str());
        let manifest = self
            .get_json_object::<ExportManifest>(&location.bucket, &manifest_key)
            .await?
            .map(|(manifest, _)| manifest)
            .ok_or_else(|| {
                StorageError::BulkExport(BulkExportError::InvalidJobState {
                    job_id: job_id.to_string(),
                    expected: "complete with manifest".to_string(),
                    actual: "complete-without-manifest".to_string(),
                })
            })?;

        Ok(manifest)
    }

    async fn list_exports(
        &self,
        tenant: &TenantContext,
        include_completed: bool,
    ) -> StorageResult<Vec<ExportProgress>> {
        let location = self.tenant_location(tenant)?;
        let prefix = location.keyspace.export_jobs_prefix();

        let mut exports = Vec::new();
        for object in self.list_objects_all(&location.bucket, &prefix).await? {
            if !object.key.ends_with("/state.json") {
                continue;
            }

            if let Some((state, _)) = self
                .get_json_object::<ExportJobState>(&location.bucket, &object.key)
                .await?
            {
                if include_completed || state.progress.status.is_active() {
                    exports.push(state.progress);
                }
            }
        }

        exports.sort_by(|a, b| b.transaction_time.cmp(&a.transaction_time));
        Ok(exports)
    }
}

#[async_trait]
impl ExportDataProvider for S3Backend {
    async fn list_export_types(
        &self,
        tenant: &TenantContext,
        request: &ExportRequest,
    ) -> StorageResult<Vec<String>> {
        let location = self.tenant_location(tenant)?;

        if !request.resource_types.is_empty() {
            let mut found = Vec::new();
            for resource_type in &request.resource_types {
                let count = self
                    .count_export_resources(tenant, request, resource_type)
                    .await?;
                if count > 0 {
                    found.push(resource_type.clone());
                }
            }
            return Ok(found);
        }

        let mut types = BTreeSet::new();
        for key in self.list_current_keys(&location, None).await? {
            if let Some(resource_type) = parse_resource_type_from_current_key(&key) {
                types.insert(resource_type);
            }
        }

        Ok(types.into_iter().collect())
    }

    async fn count_export_resources(
        &self,
        tenant: &TenantContext,
        request: &ExportRequest,
        resource_type: &str,
    ) -> StorageResult<u64> {
        let location = self.tenant_location(tenant)?;
        let keys = self
            .list_current_keys(&location, Some(resource_type))
            .await?;

        let mut count = 0u64;
        for key in keys {
            let Some((resource, _)) = self
                .get_json_object::<crate::types::StoredResource>(&location.bucket, &key)
                .await?
            else {
                continue;
            };

            if resource.is_deleted() {
                continue;
            }

            if let Some(since) = request.since {
                if resource.last_modified() < since {
                    continue;
                }
            }

            count += 1;
        }

        Ok(count)
    }

    async fn fetch_export_batch(
        &self,
        tenant: &TenantContext,
        request: &ExportRequest,
        resource_type: &str,
        cursor: Option<&str>,
        batch_size: u32,
    ) -> StorageResult<NdjsonBatch> {
        let location = self.tenant_location(tenant)?;
        let mut keys = self
            .list_current_keys(&location, Some(resource_type))
            .await?;
        keys.sort();

        let mut lines = Vec::new();
        for key in keys {
            let Some((resource, _)) = self
                .get_json_object::<crate::types::StoredResource>(&location.bucket, &key)
                .await?
            else {
                continue;
            };

            if resource.is_deleted() {
                continue;
            }

            if let Some(since) = request.since {
                if resource.last_modified() < since {
                    continue;
                }
            }

            lines.push(serde_json::to_string(resource.content()).map_err(|e| {
                StorageError::BulkExport(BulkExportError::WriteError {
                    message: format!("failed to serialize NDJSON line: {e}"),
                })
            })?);
        }

        let offset = parse_export_cursor(cursor)?;
        let start = offset.min(lines.len());
        let end = start.saturating_add(batch_size as usize).min(lines.len());

        let batch_lines = lines[start..end].to_vec();
        let is_last = end >= lines.len();
        let next_cursor = if is_last { None } else { Some(end.to_string()) };

        Ok(NdjsonBatch {
            lines: batch_lines,
            next_cursor,
            is_last,
        })
    }
}

impl S3Backend {
    /// Drives a bulk export job to completion.
    ///
    /// Iterates over all matching resource types, fetches them in batches, and
    /// writes NDJSON output parts to S3. Updates the job state object after
    /// each type completes and writes the final manifest on success.
    async fn run_export_job(
        &self,
        tenant: &TenantContext,
        job_id: &ExportJobId,
    ) -> StorageResult<()> {
        let location = self.tenant_location(tenant)?;
        let mut state = self.load_export_state(tenant, job_id).await?;

        state.progress.status = ExportStatus::InProgress;
        state.progress.started_at = Some(Utc::now());
        state.progress.error_message = None;
        state.progress.current_type = None;
        state.progress.type_progress.clear();

        self.save_export_state(tenant, job_id, &state).await?;

        let resource_types = self.list_export_types(tenant, &state.request).await?;
        let mut output_files: Vec<ExportOutputFile> = Vec::new();

        for resource_type in resource_types {
            state.progress.current_type = Some(resource_type.clone());
            self.save_export_state(tenant, job_id, &state).await?;

            let mut type_progress = TypeExportProgress::new(resource_type.clone());
            type_progress.total_count = Some(
                self.count_export_resources(tenant, &state.request, &resource_type)
                    .await?,
            );

            let mut cursor: Option<String> = None;
            let mut part_lines: Vec<String> = Vec::new();
            let mut part_number: u32 = 1;

            loop {
                let batch = self
                    .fetch_export_batch(
                        tenant,
                        &state.request,
                        &resource_type,
                        cursor.as_deref(),
                        state.request.batch_size.max(1),
                    )
                    .await?;

                for line in batch.lines {
                    part_lines.push(line);
                    if part_lines.len() >= self.config.bulk_export_part_size as usize {
                        let written = self
                            .write_export_part(
                                &location,
                                job_id,
                                &resource_type,
                                part_number,
                                &part_lines,
                            )
                            .await?;
                        output_files.push(written);
                        type_progress.exported_count += part_lines.len() as u64;
                        type_progress.cursor_state = batch.next_cursor.clone();
                        self.save_export_type_progress(&location, job_id, &type_progress)
                            .await?;
                        part_lines.clear();
                        part_number += 1;
                    }
                }

                cursor = batch.next_cursor;
                if batch.is_last {
                    break;
                }
            }

            if !part_lines.is_empty() {
                let written = self
                    .write_export_part(&location, job_id, &resource_type, part_number, &part_lines)
                    .await?;
                output_files.push(written);
                type_progress.exported_count += part_lines.len() as u64;
                part_lines.clear();
            }

            type_progress.cursor_state = None;
            self.save_export_type_progress(&location, job_id, &type_progress)
                .await?;
            state.progress.type_progress.push(type_progress);
        }

        state.progress.status = ExportStatus::Complete;
        state.progress.completed_at = Some(Utc::now());
        state.progress.current_type = None;
        state.progress.error_message = None;

        let manifest = ExportManifest {
            transaction_time: state.progress.transaction_time,
            request: format!("$export?job={}", job_id),
            requires_access_token: true,
            output: output_files,
            error: Vec::new(),
            message: None,
            extension: None,
        };

        state.manifest = Some(manifest.clone());

        let manifest_key = location.keyspace.export_job_manifest_key(job_id.as_str());
        let manifest_payload = self.serialize_json(&manifest)?;
        self.put_json_object(
            &location.bucket,
            &manifest_key,
            &manifest_payload,
            None,
            None,
        )
        .await?;

        self.save_export_state(tenant, job_id, &state).await
    }

    /// Writes a single NDJSON output part to S3 and returns an
    /// `ExportOutputFile` describing the S3 location and line count.
    async fn write_export_part(
        &self,
        location: &TenantLocation,
        job_id: &ExportJobId,
        resource_type: &str,
        part_number: u32,
        lines: &[String],
    ) -> StorageResult<ExportOutputFile> {
        let key =
            location
                .keyspace
                .export_job_output_key(job_id.as_str(), resource_type, part_number);
        let mut body = lines.join("\n");
        body.push('\n');

        self.put_bytes_object(
            &location.bucket,
            &key,
            body.as_bytes(),
            Some("application/fhir+ndjson"),
        )
        .await?;

        Ok(
            ExportOutputFile::new(resource_type, format!("s3://{}/{}", location.bucket, key))
                .with_count(lines.len() as u64),
        )
    }

    /// Returns `true` if the job state object exists in S3.
    async fn export_job_exists(
        &self,
        location: &TenantLocation,
        job_id: &ExportJobId,
    ) -> StorageResult<bool> {
        let key = location.keyspace.export_job_state_key(job_id.as_str());
        Ok(self
            .client
            .head_object(&location.bucket, &key)
            .await
            .map_err(|e| self.map_client_error(e))?
            .is_some())
    }

    /// Loads and deserialises the export job state from S3.
    ///
    /// Returns `JobNotFound` if the state object does not exist.
    async fn load_export_state(
        &self,
        tenant: &TenantContext,
        job_id: &ExportJobId,
    ) -> StorageResult<ExportJobState> {
        let location = self.tenant_location(tenant)?;
        let key = location.keyspace.export_job_state_key(job_id.as_str());
        self.get_json_object::<ExportJobState>(&location.bucket, &key)
            .await?
            .map(|(state, _)| state)
            .ok_or_else(|| {
                StorageError::BulkExport(BulkExportError::JobNotFound {
                    job_id: job_id.to_string(),
                })
            })
    }

    /// Serialises and writes the export job state to S3.
    async fn save_export_state(
        &self,
        tenant: &TenantContext,
        job_id: &ExportJobId,
        state: &ExportJobState,
    ) -> StorageResult<()> {
        let location = self.tenant_location(tenant)?;
        let key = location.keyspace.export_job_state_key(job_id.as_str());
        let payload = self.serialize_json(state)?;
        self.put_json_object(&location.bucket, &key, &payload, None, None)
            .await?;
        Ok(())
    }

    /// Transitions the export job to the `Error` state, recording the failure
    /// message in the state object.
    async fn mark_export_failed(
        &self,
        tenant: &TenantContext,
        job_id: &ExportJobId,
        message: &str,
    ) -> StorageResult<()> {
        let mut state = self.load_export_state(tenant, job_id).await?;
        state.progress.status = ExportStatus::Error;
        state.progress.completed_at = Some(Utc::now());
        state.progress.current_type = None;
        state.progress.error_message = Some(message.to_string());
        self.save_export_state(tenant, job_id, &state).await
    }

    /// Writes per-type export progress to S3 so that partial completion can be
    /// inspected before the job finishes.
    async fn save_export_type_progress(
        &self,
        location: &TenantLocation,
        job_id: &ExportJobId,
        progress: &TypeExportProgress,
    ) -> StorageResult<()> {
        let key = location
            .keyspace
            .export_job_progress_key(job_id.as_str(), &progress.resource_type);
        let payload = self.serialize_json(progress)?;
        self.put_json_object(&location.bucket, &key, &payload, None, None)
            .await?;
        Ok(())
    }
}

/// Parses the numeric offset encoded in an export batch cursor.
///
/// A `None` cursor is treated as offset `0` (start of the result set).
fn parse_export_cursor(cursor: Option<&str>) -> StorageResult<usize> {
    match cursor {
        None => Ok(0),
        Some(raw) => raw.parse::<usize>().map_err(|_| {
            StorageError::BulkExport(BulkExportError::InvalidRequest {
                message: format!("invalid export cursor: {raw}"),
            })
        }),
    }
}

/// Extracts the resource type from a `current.json` object key.
///
/// Keys follow the pattern `…/resources/<type>/<id>/current.json`; the
/// segment immediately after `resources` is the resource type.
fn parse_resource_type_from_current_key(key: &str) -> Option<String> {
    let parts: Vec<&str> = key.split('/').collect();
    let resources_idx = parts.iter().position(|segment| *segment == "resources")?;
    parts.get(resources_idx + 1).map(|s| s.to_string())
}
