from monetate_recommendations.settings import *

SECRET_KEY = 'test only secret key'  # noqa

MIGRATION_MODULES = {
    "auth": None,
    "contenttypes": None,
    "sessions": None,
    "action": None,
    "analytics": None,
    "audience": None,
    "bluekai": None,
    "campaign": None,
    "content": None,
    "dataset": None,
    "demographic": None,
    "dio": None,
    "email": None,
    "event": None,
    "idrec": None,
    "key": None,
    "location": None,
    "mauth": None,
    "merch": None,
    "milestones": None,
    "notification": None,
    "offer": None,
    "placement": None,
    "predicate": None,
    "predictive_testing": None,
    "preferences": None,
    "recs": None,
    "report": None,
    "reportv3": None,
    "retailer": None,
    "rule": None,
    "script": None,
    "segmentation": None,
    "system": None,
    "target": None,
    "trigger": None,
    "monetate_recommendations": None,
}


DATABASES['analytics'] = {
    'ENGINE': 'django.db.backends.mysql',
    'NAME': 'test_analytics',
    'HOST': 'localhost',
    'PORT': '',
    'USER': 'root',
    'PASSWORD': '',
}

DATABASES['catalog'] = {
    'ENGINE': 'django.db.backends.mysql',
    'NAME': 'test_catalog',
    'HOST': 'localhost',
    'PORT': '',
    'USER': 'root',
    'PASSWORD': '',
}


if 'SNOWFLAKE_TEST_DSN' in os.environ:
    SNOWFLAKE_QUERY_DSN = os.environ['SNOWFLAKE_TEST_DSN']
else:
    try:
        import boto3
        s3 = boto3.resource('s3')
        password_object = s3.Object('secret-monetate-dev', 'db/snowflake/test_user/password.txt')
        SNOWFLAKE_QUERY_DSN = 'snowflake://test_user:{password}@monetatedev.us-east-1/test_db/'.format(
            password=password_object.get()['Body'].read().strip())
    except Exception:
        SNOWFLAKE_QUERY_DSN = ''
SNOWFLAKE_SCHEMA = 'NOT_A_TEST_SCHEMA'
REPORTV3_SNOWFLAKE_STAGE = '@test_db.public.test_reportv3_stage_v1'
RECO_DIO_SNOWFLAKE_STAGE = '@test_db.public.test_reco_dio_stage_v1'
RECO_MERCH_SNOWFLAKE_STAGE = '@test_db.public.test_reco_merch_stage_v1'
