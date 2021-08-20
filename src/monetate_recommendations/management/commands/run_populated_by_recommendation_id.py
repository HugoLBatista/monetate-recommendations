import django
django.setup()

from django.core.management.base import BaseCommand
import monetate_recommendations.precompute_collab_algo_map as precompute_collab_algo_map


class Command(BaseCommand):
    help = "Run precompute processes by recset id"

    def add_arguments(self, parser):
        parser.add_argument('--recommendation_ids', default=None, dest='recommendation_ids', nargs='+',
                            help='process all for the specified recommendation ids', type=int)

    def handle(self, *args, **options):

        recommendation_ids = options.get('recommendation_ids')
        recommendations_by_algorithm = precompute_collab_algo_map.sort_recommendation_algo(recommendation_ids)
        for algorithm in recommendations_by_algorithm.keys():
            print('Processing {} recsets...'.format(algorithm))
            precompute_function = precompute_collab_algo_map.FUNC_MAP.get(algorithm)
            precompute_function(recommendations_by_algorithm[algorithm])
        print('Finished processing {}'.format(recommendation_ids))
