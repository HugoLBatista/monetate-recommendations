import datetime
from django.core.management.base import BaseCommand
from django.db.models import Q

from monetate.recs.models import RecommendationSet
import monetate.retailer.models as retailer_models
import monetate.dio.models as dio_models


class Command(BaseCommand):
    help = "Run precompute processes by recset id"

    def add_arguments(self, parser):
        parser.add_argument('--account_ids', default=None, dest='account_ids', nargs='+',
                            help='process all recs (collab and noncollab) for account', type=int)

    def handle(self, *args, **options):
        def _enqueue(schema_ids, retailer_id, command_time):
            for schema_id in set(schema_ids):
                try:
                    fileimport = dio_models.DioFileImport.objects.filter(
                        Q(s3_path__contains='schema-{}'.format(schema_id)) & Q(
                            s3_path__contains='retailer-{}'.format(retailer_id)),
                        status='COMPLETE',
                        is_partial_update=False,
                    ).order_by('-id')[0]
                    fileimport.upload_time = command_time
                    fileimport.status = 'PENDING'
                    fileimport.save()
                    print('schema {} pending'.format(schema_id))
                except Exception:
                    print('schema {} failed'.format(schema_id))

        account_ids = options.get('account_ids')
        now = datetime.datetime.now()
        for account_id in account_ids:
            print('processing account {}...'.format(account_id))
            account = retailer_models.Account.objects.get(id=account_id)
            retailer = account.retailer
            recsets = RecommendationSet.objects.filter(
                Q(account=account) | Q(account=None),
                archived=False,
                retailer=retailer,
            )

            catalog_ids = [dio_models.DefaultAccountCatalog.objects.get(account=6814).schema.id]
            dataset_ids = []

            for recset in recsets:
                if recset.product_catalog:
                    catalog_ids.append(recset.product_catalog.id)
                if recset.dataset:
                    dataset_ids.append(recset.dataset.id)
            print('catalog ids: {}'.format(set(catalog_ids)))
            print('dataset ids: {}'.format(set(dataset_ids)))
            _enqueue(catalog_ids, retailer.id, now)
            _enqueue(dataset_ids, retailer.id, now)

            # TODO: Once precompute is productionized we can pull from the prod s3 bucket into dev session
