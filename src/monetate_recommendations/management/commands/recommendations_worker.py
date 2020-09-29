import logging
from django.core.management import BaseCommand

from monetate_recommendations.precompute_worker import PrecomputeWorker, DEFAULTS


LOG = logging.getLogger('monetate_recommendations.precompute_worker')


class Command(BaseCommand):
    worker_kw_args = ('poll_interval', 'max_tries', 'heartbeat_interval', 'heartbeat_threshold', 'worker_max_time')

    def add_arguments(self, parser):
        parser.add_argument('--poll-interval',
                            type=int,
                            help='Time (seconds) to wait in between looking for jobs',
                            default=DEFAULTS.poll_interval)
        parser.add_argument('--max-tries',
                            type=int,
                            help='Maximum number of times to try loading a file',
                            default=DEFAULTS.max_tries)
        parser.add_argument('--heartbeat-interval',
                            type=int,
                            help='How often (seconds) to heartbeat while doing work',
                            default=DEFAULTS.heartbeat_interval)
        parser.add_argument('--heartbeat-threshold',
                            type=int,
                            help='Age of heartbeat time (seconds) before declaring that a job claim is no longer held',
                            default=DEFAULTS.heartbeat_threshold)
        parser.add_argument('--worker-max-time',
                            type=int,
                            help=('Max time (seconds) that a worker can run. After this, it will exit instead of '
                                  'getting more work.'),
                            default=DEFAULTS.worker_max_time)

    def handle(self, *args, **options):
        try:
            worker_opts = {k: v for k, v in options.items() if k in self.worker_kw_args}
            worker = PrecomputeWorker(**worker_opts)
            worker.do_work()
        except Exception as e:
            LOG.exception('Worker threw an uncaught exception: {}'.format(e))
            raise
