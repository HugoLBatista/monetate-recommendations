STATUS_PENDING = 'PENDING'
STATUS_PROCESSING = 'PROCESSING'
STATUS_COMPLETE = 'COMPLETE'
STATUS_SKIPPED = 'SKIPPED'  # Files may be skipped if they are older than the latest data loaded into session
STATUS_SYS_ERROR = 'SYSTEM_ERROR'
STATUS_TIMEOUT_ERROR = 'TIMEOUT_ERROR'
STATUS_CHOICES = (
    (STATUS_PENDING, 'Looking for new recommendations'),
    (STATUS_PROCESSING, 'Processing recommendation'),
    (STATUS_COMPLETE, 'Completed successfully'),
    (STATUS_SKIPPED, 'Not processed because it is older than current threshold'),
    (STATUS_TIMEOUT_ERROR, 'Worker thread has exceeded 8 hour limit'),
)

RETRYABLE_STATES = [
    STATUS_PROCESSING,
    STATUS_TIMEOUT_ERROR,
]

COMPLETED_CHOICES = (
    (True, 'Process completed'),
    (False, 'Process not completed'),
)

TEXT_FIELD_MAX_LEN = 2**20

GEO_TARGET_COLUMNS = {
    'country': ["country_code"],
    'region': ["country_code", "region"]
}
