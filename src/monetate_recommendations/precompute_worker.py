"""
Recommendations Precompute Worker
=================================

Polls config for recommendations that it can process and does the following in a loop:
  - Find and claim an eligible recommendation (see docstrings for query_recommendations and claim_recommendation)
  - Fork a thread that does the processing work. Meanwhile, the main thread heartbeats by periodically
    updating the recommendation's heartbeat_time.
  - Once the thread exits (or times out), update the recommendation's status with the outcome.

Usage
-----
    worker = PrecomputeWorker()
    worker.do_work()
"""

import datetime
import os
import time
import threading
import traceback

from django.utils import timezone
from django.db.models import Q
from monetate_monitoring import log
from monetate.recs.models import RecommendationsPrecompute, PrecomputeQueue
import monetate.recs.precompute_constants as precompute_constants
import precompute_algo_map as precompute_algo_map
import precompute_collab_algo_map as precompute_collab_algo_map

log.configure_script_log('recommendations_worker')


class DEFAULTS(object):
    poll_interval = 10
    max_tries = 3
    heartbeat_interval = 60
    heartbeat_threshold = 300
    worker_max_time = 28800


def get_hostname():
    # https://docs.python.org/2/library/os.html#os.uname
    import socket
    return socket.gethostname()


class JobTimeoutError(Exception):
    """Raised when a job takes too long to process.  Do not catch this; let it kill the process."""
    pass


class PrecomputeThread(threading.Thread):

    def __init__(self, recommendation):
        self.recommendation = recommendation
        self.result = None
        self.exception = None
        self.message = None
        self.traceback = ""
        super(PrecomputeThread, self).__init__()
        self.daemon = True
        self.connector = None

    def run(self):
        try:
            algorithm = self.recommendation.recset.algorithm
            if algorithm in precompute_algo_map.FUNC_MAP.keys():
                self.result = precompute_algo_map.FUNC_MAP[algorithm]([self.recommendation.recset])[0]
            else:
                self.message = 'invalid precompute algorithm {}'.format(algorithm)
                self.recommendation.status = precompute_constants.STATUS_SKIPPED
                self.recommendation.save()
        except Exception as e:
            self.exception = e
            self.traceback = traceback.format_exc()


class PrecomputeCombinedThread(threading.Thread):

    def __init__(self, recommendation):

        super(PrecomputeCombinedThread, self).__init__()
        self.recset_group = recommendation
        self.result = None
        self.exception = None
        self.message = None
        self.traceback = ""
        self.daemon = True
        self.connector = None


    def run(self):
        try:
            algorithm = self.recset_group.algorithm
            if algorithm in precompute_collab_algo_map.FUNC_MAP.keys():
                self.result = precompute_collab_algo_map.initialize_collab_algorithm([self.recset_group], algorithm)[0]
            else:
                self.message = 'invalid precompute algorithm {}'.format(algorithm)
                self.recset_group.status = precompute_constants.STATUS_SKIPPED
                self.recset_group.save()
        except Exception as e:
            self.exception = e
            self.traceback = traceback.format_exc()


class PrecomputeWorker(object):
    """
    Defines a recommendations precompute worker.
    The worker queries config db, receiving all recs that need to be processed and passes them to a child thread.
    The child thread constructs a snowflake query. The results are passed back to the worker and it updates the db.

    :param poll_interval: How many seconds the Worker should wait before looking for new recs.
    :param max_tries: How many times a worker should attempt to process a rec before marking as erred.
    :param heartbeat_interval: How often to heartbeat while doing work.
    :param heartbeat_threshold: How old a heartbeat needs to be before assuming its worker died.
    :param worker_max_time: Max time that a worker can run: it will exit upon completion of current job.
    """

    def __init__(self, poll_interval=DEFAULTS.poll_interval, max_tries=DEFAULTS.max_tries,
                 heartbeat_interval=DEFAULTS.heartbeat_interval, heartbeat_threshold=DEFAULTS.heartbeat_threshold,
                 worker_max_time=DEFAULTS.worker_max_time, use_combined_queue=False):
        self.poll_interval = poll_interval
        self.max_tries = max_tries
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_threshold = heartbeat_threshold
        self.worker_max_time = worker_max_time
        self.worker_start_time = time.time()
        self.worker_id = '{}-{}-{}'.format(get_hostname(), os.getpid(), int(self.worker_start_time))
        self.recommendation = None
        self.attempts = 0
        self.use_combined_queue = use_combined_queue

    def log(self, msg, level=log.LOG_INFO):
        if self.recommendation is not None:
            log.get_error_log().log(format('recommendations precompute {}: {}'.format(self.recommendation.id, msg)),
                                    priority=level)
            self.recommendation.append_to_status_log('{}: worker {}: {}\n'.format(timezone.now(), self.worker_id, msg))
        else:
            log.get_error_log().log(msg, priority=level)

    def do_work(self):
        """
        Start the worker.
        Runs in a loop until exceeds worker_max_time.
        :return: None
        """
        worker_exit_time = self.worker_start_time + self.worker_max_time
        self.log('Worker starting; will exit at {}'.format(datetime.datetime.utcfromtimestamp(worker_exit_time)))
        while True:
            if time.time() > worker_exit_time:
                self.log('Worker has been alive longer than {} seconds. Exiting.'.format(self.worker_max_time))
                break
            self.recommendation = None
            self.poll()
            if self.recommendation is None:
                # Didn't find any work to do; wait a bit before looking for more
                time.sleep(self.poll_interval)

    def poll(self):
        """
        Try to find work. If work exists, do it.
        """
        self.log('Looking for new recommendations...')
        recs_qs = self.query_recommendations()
        if not recs_qs:
            return
        if not self.claim_recommendation(recs_qs):
            return
        self.log('Claimed rec {}'.format(self.recommendation.id))
        try:
            self.recommendation.attempts += 1
            self.recommendation.precompute_start_time = timezone.now()
            thread = self.run_work_thread()  # This thread does the actual work
            self.handle_thread_result(thread)
        finally:
            # Once we get here, file should no longer be in processing state
            # If it is, we messed something up
            if self.recommendation.status == precompute_constants.STATUS_PROCESSING:
                self.log('Unexpectedly still in processing', level=log.LOG_ERR)
                self.recommendation.status = precompute_constants.STATUS_SYS_ERROR
            self.recommendation.precompute_end_time = timezone.now()
            self.recommendation.processing_time_seconds = int((self.recommendation.precompute_end_time - self.recommendation.precompute_start_time).total_seconds())
            self.log('Finished processing recommendation {} -- elapsed time {}'.format(self.recommendation.id, self.recommendation.processing_time_seconds))
            self.recommendation.save()

    def query_recommendations(self):
        """
        Query recommendations that are eligible for processing.
        A recommendation is eligible if:
          - attempts < max_tries, AND
          - one of the following:
            - Status is PENDING, OR
            - Status is RETRYABLE_STATES, AND heartbeat_time is null or older than heartbeat_threshold (In this
              instance, we are assuming that a worker died hard without having a chance to clean itself up.)

        :return: Queryset
        """
        heartbeat_old_time = timezone.now() - datetime.timedelta(seconds=self.heartbeat_threshold)
        recs_pending = Q(status=precompute_constants.STATUS_PENDING)
        recs_retryable = ((Q(heartbeat_time__lt=heartbeat_old_time) | Q(heartbeat_time=None)) &
                          Q(status__in=precompute_constants.RETRYABLE_STATES))
        if self.use_combined_queue:
            return PrecomputeQueue.objects.filter(recs_pending | recs_retryable, attempts__lt=self.max_tries)
        else:
            return RecommendationsPrecompute.objects.filter(recs_pending | recs_retryable, attempts__lt=self.max_tries)

    def claim_recommendation(self, recs_qs):
        """
        Attempt to claim an eligible recommendation.
        If successful, set self.recommendation to the claimed recommendation and return True.
        Otherwise, set self.recommendation to None and return False.

        This works as follows:
          - Query the first 10 eligible recommendations (The number 10 is arbitrary; just prevents us from fetching
            the entire table every time).
          - For each of those, try to update it by setting its status to PROCESSING and its heartbeat_time to now().
            - If the update actually updates a row, then we have successfully claimed the recommendation.
            - If the update does not update a row, then another worker got to it before us. Try the next one.
          - Return True if claimed a recommendation or False if failed claiming all 10.
        Thus we never need to acquire a lock on the table to make the claim.

        :return: Bool
        """
        self.recommendation = None
        for rec in recs_qs.order_by('id')[:10]:
            if self.use_combined_queue:
                this_rec_qs = PrecomputeQueue.objects.filter(id=rec.id, status=rec.status,
                                                              heartbeat_time=rec.heartbeat_time)
            else:
                this_rec_qs = RecommendationsPrecompute.objects.filter(id=rec.id, status=rec.status,
                                                                       heartbeat_time=rec.heartbeat_time)
            rows_updated = this_rec_qs.update(status=precompute_constants.STATUS_PROCESSING,
                                              heartbeat_time=timezone.now())
            if rows_updated:
                # I successfully claimed a rec
                # NB: since update() does not call save() we need to re-query the rec from the DB
                rec.refresh_from_db()
                self.recommendation = rec
                self.log('Claimed recommendation', log.LOG_DEBUG)
                break
            else:
                # Someone got in and claimed it before me.  Try the next one.
                self.log('Tried and failed to claim recommendation {}'.format(rec.id), log.LOG_DEBUG)
        else:
            self.log('Did not claim any recommendation')
            return False
        return True

    def run_work_thread(self):
        """
        Run work in a child thread.
        In the main thread, heartbeat against the recs table to keep our claim current.
        Return the completed thread.
        """
        if self.use_combined_queue:
            thread = PrecomputeCombinedThread(self.recommendation)
            thread.start()
        else:
            thread = PrecomputeThread(self.recommendation)
            thread.start()

        while thread.is_alive():
            self.heartbeat()
            thread.join(timeout=self.heartbeat_interval)
        if thread.is_alive():
            # If the thread is still alive at this point, assume it's never going to complete
            # or run its cleanup tasks, so make sure we do them here.
            err_msg = 'Recommendation {} snowflake query timed out'.format(self.recommendation.id)
            self.log(err_msg)
            self.recommendation.status = precompute_constants.STATUS_TIMEOUT_ERROR
            if thread.connector is not None:
                thread.connector.cleanup()
            raise JobTimeoutError(err_msg)
        return thread

    def heartbeat(self):
        """Update heartbeat_time to keep our claim on the file alive"""
        hb_time = self.recommendation.heartbeat_time = timezone.now()
        start_time = self.recommendation.precompute_start_time
        if start_time is not None:
            self.log('heartbeat {}'.format(hb_time - start_time))
        self.recommendation.save()

    def handle_thread_result(self, thread):
        """Set status and log according to the results of the work"""
        if thread.exception is not None:
            self.recommendation.status = precompute_constants.STATUS_SYS_ERROR
            self.log('Threw an error during processing: {}'.format(thread.traceback), level=log.LOG_DEBUG)
            raise thread.exception
        else:
            self.recommendation.status = precompute_constants.STATUS_COMPLETE
            self.recommendation.process_complete = True
            self.log('products returned: {}'.format(thread.result))
            self.recommendation.products_returned = sum(thread.result) if thread.result else 0
            if thread.message:
                self.log(thread.message)
