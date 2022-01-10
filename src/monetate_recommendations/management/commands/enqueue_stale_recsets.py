import datetime
from django.core.management.base import BaseCommand
from django.db.models import Q
from django.utils import timezone
from monetate.common import log
import monetate.recs.models as recs_models
import monetate.recs.precompute_constants as precompute_constants
import monetate.retailer.models as retailer_models

log.configure_script_log('enqueue_stale_recsets')


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('--hours', default=24, dest='hours', nargs='+',
                            help='Number of hours before a recset is considered stale', type=int)

    def get_account(self, recset, account=None):
        # anytime a recset has a market, account_id should be None
        if recset.is_market_or_retailer_driven_ds:
            return None
        # if not market and not retailer level, return account_id from RecommendationSet table
        elif not recset.is_retailer_tenanted:
            return recset.account
        # if not market but retailer level, return the account_id of current account
        return account

    def enqueue_precompute_collab(self, recset, account=None):
        _, created = recs_models.PrecomputeQueue.objects.get_or_create(
            account=self.get_account(recset, account),
            market=recset.market,
            retailer=recset.retailer if recset.retailer_market_scope else None,
            algorithm=recset.algorithm,
            lookback_days=recset.lookback_days,
            defaults={
                'status': precompute_constants.STATUS_PENDING,
                'process_complete': False,
                'products_returned': 0,
                'attempts': 0,
                'precompute_enqueue_time': timezone.now()
            }
        )
        return created

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
            (Q(algorithm__in=recs_models.RecommendationSet.NONCOLLAB_ALGORITHMS)) & \
            (Q(account__in=precompute_accounts) | (Q(account__isnull=True) & Q(retailer__in=precompute_retailers))),
            archived=False,
        )

        updated_recsets = []
        created_recsets = []
        for recset in precompute_recsets:
            precompute_recsets_status = recs_models.RecommendationsPrecompute.objects.filter(recset=recset)
            heartbeat_threshold = 300
            heartbeat_old_time = timezone.now() - datetime.timedelta(seconds=heartbeat_threshold)
            # exclude recsets that are pending or currently processing and have a heartbeat in the past 5 minutes or
            # have a heartbeat_time=None.
            recs_to_exclude = (Q(status=precompute_constants.STATUS_PENDING) |
                               (Q(status=precompute_constants.STATUS_PROCESSING) &
                                (Q(heartbeat_time__gt=heartbeat_old_time) | Q(heartbeat_time=None))))
            if precompute_recsets_status:
                updated = precompute_recsets_status.filter(
                    precompute_end_time__lt=stale_time,
                ).exclude(
                    recs_to_exclude
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
        log.log_info('stale precompute entries updated: {}'.format(updated_recsets))
        log.log_info('new precompute entries created: {}'.format(created_recsets))

        # enqueue precompute collab
        precompute_collab_feature = retailer_models.ACCOUNT_FEATURES.ENABLE_COLLAB_RECS_PRECOMPUTE_MODELING
        precompute_collab_accounts = retailer_models.Account.objects.filter(
            accountfeature__feature_flag__name=precompute_collab_feature,
            archived=False,
        )
        precompute_collab_retailers = retailer_models.Retailer.objects.filter(
            account__accountfeature__feature_flag__name=precompute_collab_feature,
        )
        precompute_collab_recsets = recs_models.RecommendationSet.objects.filter(
            (Q(algorithm__in=recs_models.RecommendationSet.PRECOMPUTE_COLLAB_ALGORITHMS)) &
            (Q(account__in=precompute_collab_accounts) | \
            (Q(account__isnull=True) & Q(retailer__in=precompute_collab_retailers))),
            archived=False,
        )
        created_collab_queue_entries = 0
        for recset in precompute_collab_recsets:
            # if retailer level and not market, need to create a queue entry for each account
            if recset.is_retailer_tenanted and not recset.is_market_or_retailer_driven_ds:
                account_ids = \
                    retailer_models.Account.objects.filter(retailer_id=recset.retailer_id,
                                                           accountfeature__feature_flag__name=precompute_collab_feature,
                                                           archived=False)
                for account_id in account_ids:
                    created = self.enqueue_precompute_collab(recset, account_id)
                    created_collab_queue_entries += 1 if created else 0
            else:
                created = self.enqueue_precompute_collab(recset)
                created_collab_queue_entries += 1 if created else 0
        log.log_info('Number of precompute combined queue entries created: {}'.format(created_collab_queue_entries))
        # updating entries for precompute combined queue
        updated_recsets_groups = recs_models.PrecomputeQueue.objects.filter(
            precompute_end_time__lt=stale_time,
        ).exclude(
            status=precompute_constants.STATUS_PENDING
        ).update(
            status=precompute_constants.STATUS_PENDING,
            process_complete=False,
            products_returned=0,
            attempts=0,
        )
        if updated_recsets_groups:
            log.log_info("stale precompute combined queue entries updated {}".format(updated_recsets_groups))