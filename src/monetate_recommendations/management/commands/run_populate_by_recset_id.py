import django
django.setup()

from django.core.management.base import BaseCommand
import monetate_recommendations.precompute_algo_map as precompute_algo_map
from monetate.recs.models import RecommendationSet


class Command(BaseCommand):
    help = "Run precompute processes by recset id"

    def add_arguments(self, parser):
        parser.add_argument('--recset_ids', default=None, dest='recset_ids', nargs='+',
                            help='process all for the specified recset ids', type=int)

    def handle(self, *args, **options):
        recset_ids = options.get('recset_ids')
        for recset_id in recset_ids:
            recset = RecommendationSet.objects.get(id=recset_id)
            precompute_function = precompute_algo_map.FUNC_MAP.get(recset.algorithm)
            if recset and precompute_function:
                print('Processing recset {}...'.format(recset_id))
                precompute_function(recset)
            else:
                print('Could not process recset {}...'.format(recset_id))
        print('Finished processing {}'.format(recset_ids))
        self.stdout.write("Unterminated line", ending='')
