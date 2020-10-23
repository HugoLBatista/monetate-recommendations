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
    author='Kibo Inc.',
    author_email='monetate_recommendations-team@monetate.com',
    classifiers=[],
    install_requires=[
        "Django>=1.11,<2.0",
        "monetate-tenant>=0.5",
        "monetate-recs==0.6",
        "aws-encryption-sdk==1.2.0",
        "azure-storage-blob==2.1.0",
        "bandit-updater==2.5.5",
        "beautifulsoup4==4.6.3",
        "Cerberus==1.2",
        "ciso8601==1.0.8",
        "css-parser==1.0.4",
        "django-json-rpc==0.7.2",
        "djangorestframework==3.5.4",
        "FormEncode==1.3.0",
        "GeoIP-Python==1.2.7",
        "geoip2==2.4.0",
        "googleads==19.0.0",
        "ipaddress==1.0.17",
        "isodate==0.6.0",
        "Jinja2==2.10.3",
        "jsonpath-rw==1.4.0",
        "maxminddb==1.2.1",
        "mysqlclient==1.3.14",
        "pandas==0.24.2",
        "paramiko==1.18.5",
        "pycrypto==2.6.1",
        "snowflake-sqlalchemy==1.1.18",
    ],
    extras_require={
        "test": [
            "bravado>=8.4.0,<8.5",
            "coverage>=4.5.1,<4.6",
            "factory_boy==2.4.1",
            "hypothesis>=3.30.0,<3.31",
            "jsonpath-rw>=1.4.0,<1.5",
            "jsonschema>=2.6.0,<2.7",
            "mock>=2.0,<2.1",
            "moto<=1.3.6",
            "parameterized>=0.6.1,<0.7",
            # pin prospector / pylint version to avoid excess lint strictness
            "prospector==0.12.4",
            "pylint==1.6.4",
            "pylint_django==0.8.0",  # last working build to support Python 2 (0.8.1 doesn't work)
            "pylint-plugin-utils<0.6",  # version 0.6 incompatible with pylint==1.6.4
            "requests>=2.10.0,<2.11",
        ]
    }
)
