import django
django.setup()

from django.core.management.base import BaseCommand
import monetate_recommendations.precompute_collab_algo_map as precompute_collab_algo_map
from monetate_recommendations.precompute_collab_algo_map import initialize_collab_algorithm


class Command(BaseCommand):
    help = "Run precompute processes by recset id"

    def add_arguments(self, parser):
        parser.add_argument('--queue_entry_ids', default=None, dest='queue_entry_ids', nargs='+',
                            help='process all for the specified recommendation ids', type=int)

    def handle(self, *args, **options):

        queue_entry_ids = options.get('queue_entry_ids')
        queue_by_algorithm = precompute_collab_algo_map.sort_recommendation_algo(queue_entry_ids)
        for algorithm in queue_by_algorithm.keys():
            print('Processing {} recsets...'.format(algorithm))
            initialize_collab_algorithm(queue_by_algorithm[algorithm], algorithm)
        print('Finished processing {}'.format(queue_entry_ids))
