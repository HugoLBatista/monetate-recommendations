import datetime
from django.core.management.base import BaseCommand
from django.db.models import Q
from monetate.common import log
import monetate.recs.models as recs_models
import monetate.recs.precompute_constants as precompute_constants
import monetate.retailer.models as retailer_models

log.configure_script_log('enqueue_stale_recsets')


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('--hours', default=24, dest='hours', nargs='+',
                            help='Number of hours before a recset is considered stale', type=int)

    def handle(self, *args, **options):
        hours = options.get('hours', 24)
        stale_time = datetime.datetime.now() - datetime.timedelta(hours=hours)
        precompute_feature = retailer_models.ACCOUNT_FEATURES.ENABLE_NONCOLLAB_RECS_PRECOMPUTE
        precompute_accounts = retailer_models.Account.objects.filter(
            accountfeature__feature_flag__name=precompute_feature,
            archived=False,
        )
        precompute_retailers = retailer_models.Retailer.objects.filter(
            account__accountfeature__feature_flag__name=precompute_feature,
        )
        precompute_recsets = recs_models.RecommendationSet.objects.filter(
            Q(account__in=precompute_accounts) | (Q(account__isnull=True) & Q(retailer__in=precompute_retailers)),
            archived=False,
        )

        updated_recsets = []
        created_recsets = []
        updated_recsets_group = []
        precompute_recsets_group = recs_models.PrecomputeQueue.onbjects.get.all()
        for recset in precompute_recsets:
            precompute_recsets_status = recs_models.RecommendationsPrecompute.objects.filter(recset=recset)
            if precompute_recsets_status:
                updated = precompute_recsets_status.filter(
                    precompute_end_time__lt=stale_time,
                ).exclude(
                    status=precompute_constants.STATUS_PENDING,
                ).update(
                    status=precompute_constants.STATUS_PENDING,
                    process_complete=False,
                    products_returned=0,
                    attempts=0,
                )
                if updated:
                    updated_recsets.append(precompute_recsets_status[0].recset.id)
            else:
                precompute_recset_status = recs_models.RecommendationsPrecompute.objects.create(
                    recset=recset,
                    status=precompute_constants.STATUS_PENDING,
                    process_complete=False,
                    products_returned=0,
                    attempts=0,
                )
                created_recsets.append(precompute_recset_status.recset.id)
        for recset_group in precompute_recsets_group:
            if recset_group:
                updated = recset_group.filter(
                    precompute_end_time__lt=stale_time,
                ).exclude(
                    status=precompute_constants.STATUS_PENDING
                ).update(
                    status=precompute_constants.STATUS_PENDING,
                    process_complete=False,
                    products_returned=0,
                    attempts=0,
                )
                if updated:
                    updated_recsets_group.append(recset_group[0].recset.id)

        log.log_info('stale precompute entries updated: {}'.format(updated_recsets))
        log.log_info('new precompute entries created: {}'.format(created_recsets))