import os

from codecs import open

from setuptools import setup, find_packages, Command

here = os.path.abspath(os.path.dirname(__file__))

# Get the long description from the README file
with open(os.path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

# Get the current version from the VERSION file
with open(os.path.join(here, "src", "monetate_recommendations", "VERSION")) as version_file:
    version = version_file.read().strip()

setup(
    name='monetate_recommendations',
    version=version,
    packages=find_packages("src"),
    package_dir={"": "src"},
    include_package_data=True,
    description='Monetate recommendations precompute service',
    long_description=long_description,
    url='https://github.com/monetate/monetate-recommendations',
    author='Monetate Inc.',
    author_email='monetate_recommendations-team@monetate.com',
    classifiers=[],
    install_requires=[
        # monetate
        "monetate-monitoring",
        "monetate-profile",
        "monetate-recs>=1.5.0",

        # django
        "Django>=1.11,<2.0",

        # mysql
        "mysqlclient~=1.3.14",

        # snowflake
        "cffi<1.14",  # snowflake-connector-python wants cffi <1.14,>=1.9
        "cryptography<3.0.0",  # snowflake-connector-python wants cryptography <3.0.0,>=1.8.2
        "idna<2.9",  # snowflake-sqlalchemy installs idna <3.0.0, requests 2.22.0 wants idna <2.9
        "pyOpenSSL<20.0.0",  # pyOpenSSL >=20.0.0 installs cryptography >3.2, sqlalchemy wants cryptography <3.2
        "pytz<2021.0",  # snowflake-connector-python wants pytz<2021.0
        "snowflake-sqlalchemy~=1.1.14",
        "snowflake-connector-python<2.2.0",  # dropped python 2.7 support in 2.2.0
        "SQLAlchemy<1.2",  # SQLAlchemy<2.0

        # false dependencies
        "Babel<2.10",  # monetate.retailer.utils.format_currency(), dropped python 2.7 support in 2.10
        "boto",  # monetate.common.warehouse.sqlalchemy_warehouse
        "django-localflavor<3.0",  # monetate.retailer.models - USStateField, dropped django 1.11 support in 3.0
        "jsmin<3.0.0",  # monetate.retailer.models.ThirdPartyReport.save(), dropped python 2.7 support in 3.0.0
        "monetate-caching",  # monetate.retailer.models - invalidate_bucket
        "Pillow<7.0.0",  # monetate.retailer.models - monetate.retailer.creative, dropped python 2.7 support in 7.0.0
    ],
    extras_require={
        "test": [
            "mock>=2.0,<2.1",
            "monetate-s3",
        ]
    }
)
