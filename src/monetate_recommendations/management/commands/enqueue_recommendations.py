from django.core.management.base import BaseCommand
from django.db.models import Q
from monetate.recs.models import RecommendationSet
from monetate.retailer.models import Account
from monetate_recommendations.models import RecommendationsPrecompute
import monetate_recommendations.constants as precompute_constants


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('--account_ids', default=None, dest='account_ids', nargs='+',
                            help='process all noncollabs recs for the specified account ids', type=int)
        parser.add_argument('--recset_ids', default=None, dest='recset_ids', nargs='+',
                            help='process the specified recset ids', type=int)

    def handle(self, *args, **options):
        recset_ids = options.get('recset_ids')
        account_ids = options.get('account_ids')
        recsets_to_process = []
        recsets_enqueued = []

        if recset_ids:
            recsets_to_process += list(RecommendationSet.objects.filter(
                id__in=recset_ids,
                algorithm__in=RecommendationSet.NONCOLLAB_ALGORITHMS,
            ))

        if account_ids:
            for account_id in account_ids:
                retailer_id = Account.objects.get(id=account_id).retailer.id
                recsets_to_process += list(RecommendationSet.objects.filter(
                    Q(account_id=None) | Q(account_id=account_id),
                    retailer_id=retailer_id,
                    algorithm__in=RecommendationSet.NONCOLLAB_ALGORITHMS,
                ))
        for recset in recsets_to_process:
            RecommendationsPrecompute.objects.get_or_create(
                recset=recset,
                status=precompute_constants.STATUS_PENDING,
                process_complete=False,
                products_returned=0,
            )
            recsets_enqueued.append(recset.id)

        print('enqueued recsets: {}'.format(recsets_enqueued))
