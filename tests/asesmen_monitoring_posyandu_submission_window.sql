select
    submission_id,
    submission_start_at,
    submission_end_at
from {{ ref('asesmen_monitoring_posyandu') }}
where submission_start_at is not null
  and submission_end_at is not null
  and submission_end_at < submission_start_at
