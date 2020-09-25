from django.db import models
from . import constants
from monetate.recs.models import RecommendationSet


class RecommendationsPrecompute(models.Model):
    """
    Represents recommendations that need to be processed by the precompute worker.
    The model keeps track of the recommendation processing state and errors.
    """
    class Meta(object):
        db_table = 'recs_recommendationprecompute'

    recset = models.ForeignKey(RecommendationSet)
    status = models.CharField(max_length=20, choices=constants.STATUS_CHOICES, default=constants.STATUS_PENDING)
    attempts = models.IntegerField(default=0)
    status_log = models.TextField(blank=True)
    processing_time_seconds = models.PositiveIntegerField(null=True) # How long did processing take
    precompute_start_time = models.DateTimeField(null=True)
    precompute_end_time = models.DateTimeField(null=True)
    heartbeat_time = models.DateTimeField(null=True) # Last heartbeat time
    process_complete = models.PositiveSmallIntegerField(choices=constants.COMPLETED_CHOICES, default=False)
    products_returned = models.IntegerField(default=0) # Number of rows returned from snowflake query
    snowflake_exceptions = models.CharField(max_length=5000, blank=True) # Populated if error occurs while querying snowflake

    def append_to_status_log(self, message):
        """Append a message to the status log, making sure that the field does not exceed its maximum length."""
        # Note that max_length in a TextField does not actually enforce that anywhere
        # https://docs.djangoproject.com/en/1.11/ref/models/fields/#textfield
        self.status_log += message
        if len(self.status_log) > constants.TEXT_FIELD_MAX_LEN:
            self.status_log = self.status_log[:constants.TEXT_FIELD_MAX_LEN]
