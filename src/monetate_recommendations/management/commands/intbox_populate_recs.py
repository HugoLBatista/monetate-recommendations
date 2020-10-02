from django.core.management.base import BaseCommand
from django.db.models import Q

from monetate.recs.models import RecommendationSet
import monetate.retailer.models as retailer_models


class Command(BaseCommand):
    help = "Run precompute processes by recset id"

    def add_arguments(self, parser):
        parser.add_argument('--account_ids', default=None, dest='account_ids', nargs='+',
                            help='process all recs (collab and noncollab) for account', type=int)

    def handle(self, *args, **options):
        account_ids = options.get('account_ids')
        for account_id in account_ids:
            print('processing account {}...'.format(account_id))
            account = retailer_models.Account.objects.get(id=account_id)
            retailer = account.retailer
            recsets = RecommendationSet.objects.filter(
                Q(account=account) | Q(account=None),
                archived=False,
                retailer=retailer,
            )
            print('recsets: {}'.format(recsets))  # TODO: remove this

            # what is naming convention for catalog vs nightly sets

            # get catalog and nightly generated datasets from dataset_diofileimport and update status to processing

            # filter noncollab recsets and process after productionized (will have prod data in s3)
