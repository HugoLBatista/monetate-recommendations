"""
Trimmed down version of monetate.test.warehouse_utils.
"""

import datetime
import time
import monetate.dio.models as dio_models
import monetate_caching.cache as retailer_cache
import monetate.retailer.models as retailer_models

LONG_AGO = datetime.datetime(1970, 1, 1)


def create_account(with_metrics=False, session_cutover_time=None):
    with retailer_cache.invalidation_context():
        retailer = retailer_models.Retailer.objects.create(
            name="Redshift integration test"
        )
        account = retailer_models.Account.objects.create(
            retailer=retailer,
            name="a-xxxxxxxx",
            instance="t",
            domain="t{:.6f}.redshift.example.com".format(time.time()),
            timezone="America/New_York",
            currency="USD",
            session_cutover_time=session_cutover_time
        )
    if with_metrics:
        pass
        # account.create_default_metrics()
    return account


def create_default_catalog_schema(account):
    with retailer_cache.invalidation_context():
        schema = dio_models.Schema.objects.create(retailer_id=account.retailer.id)
        default_catalog = dio_models.DefaultAccountCatalog.objects.create(
            account=account,
            schema=schema
        )
        default_catalog.save()
        return default_catalog
