from typing import Optional

from pydantic import BaseModel
from pydantic_extra_types.pendulum_dt import DateTime


class FileLoadLog(BaseModel):
    id: Optional[int] = None
    source_filename: str
    started_at: DateTime
    duplicate_skipped: Optional[bool] = None
    # archive copy phase
    archive_copy_started_at: Optional[DateTime] = None
    archive_copy_ended_at: Optional[DateTime] = None
    archive_copy_success: Optional[bool] = None
    # reading phase
    read_started_at: Optional[DateTime] = None
    read_ended_at: Optional[DateTime] = None
    read_success: Optional[bool] = None
    # validating phase
    validate_started_at: Optional[DateTime] = None
    validate_ended_at: Optional[DateTime] = None
    validate_success: Optional[bool] = None
    # stage load phase
    write_started_at: Optional[DateTime] = None
    write_ended_at: Optional[DateTime] = None
    write_success: Optional[bool] = None
    # audit phase
    audit_started_at: Optional[DateTime] = None
    audit_ended_at: Optional[DateTime] = None
    audit_success: Optional[bool] = None
    # merge phase
    publish_started_at: Optional[DateTime] = None
    publish_ended_at: Optional[DateTime] = None
    publish_success: Optional[bool] = None
    # summary
    ended_at: Optional[DateTime] = None
    records_read: Optional[int] = None
    validation_errors: Optional[int] = None
    records_written_to_stage: Optional[int] = None
    target_inserts: Optional[int] = None
    target_updates: Optional[int] = None
    success: Optional[bool] = None
    error_type: Optional[str] = None
