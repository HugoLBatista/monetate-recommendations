import django
django.setup()

from django.core.management.base import BaseCommand
import monetate_recommendations.precompute_algo_map as precompute_algo_map


class Command(BaseCommand):
    help = "Run precompute processes by recset id"

    def add_arguments(self, parser):
        parser.add_argument('--recset_ids', default=None, dest='recset_ids', nargs='+',
                            help='process all for the specified recset ids', type=int)

    def handle(self, *args, **options):
        recset_ids = options.get('recset_ids')
        recsets_by_algorithm = precompute_algo_map.sort_recsets_by_algorithm(recset_ids)
        for algorithm in recsets_by_algorithm.keys():
            print('Processing {} recsets...'.format(algorithm))
            precompute_function = precompute_algo_map.FUNC_MAP.get(algorithm)
            precompute_function(recsets_by_algorithm[algorithm])
        print('Finished processing {}'.format(recset_ids))
