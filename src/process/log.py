from typing import Optional

from pydantic import BaseModel
from pydantic_extra_types.pendulum_dt import DateTime


class FileLoadLog(BaseModel):
    id: Optional[int] = None
    source_filename: str
    started_at: DateTime
    duplicate_skipped: Optional[bool] = None
    archive_copy_started_at: Optional[DateTime] = None
    archive_copy_ended_at: Optional[DateTime] = None
    archive_copy_success: Optional[bool] = None
    processing_started_at: Optional[DateTime] = None
    processing_ended_at: Optional[DateTime] = None
    processing_success: Optional[bool] = None
    stage_load_started_at: Optional[DateTime] = None
    stage_load_ended_at: Optional[DateTime] = None
    stage_load_success: Optional[bool] = None
    audit_started_at: Optional[DateTime] = None
    audit_ended_at: Optional[DateTime] = None
    audit_success: Optional[bool] = None
    merge_started_at: Optional[DateTime] = None
    merge_ended_at: Optional[DateTime] = None
    merge_success: Optional[bool] = None
    ended_at: Optional[DateTime] = None
    records_processed: Optional[int] = None
    validation_errors: Optional[int] = None
    records_stage_loaded: Optional[int] = None
    target_inserts: Optional[int] = None
    target_updates: Optional[int] = None
    success: Optional[bool] = None
    error_type: Optional[str] = None
