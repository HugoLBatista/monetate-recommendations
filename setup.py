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
        "chardet==3.0.4",
        "css-parser==1.0.4",
        "Django>=1.11,<2.0",
        "django-json-rpc==0.7.2",
        "djangorestframework==3.5.4",
        #"monetate-bandit==1.8.1",
        #"monetate-tenant==0.6",
        #"monetate-recs==1.2",
        "anyjson==0.3.3",
        "aws-encryption-sdk==1.2.0",
        "attrs==19.1.0",
        "azure-common==1.1.25",
        "azure-nspkg==3.0.2",
        "azure-storage-blob==2.1.0",
        "azure-storage-common==2.1.0",
        "azure-storage-nspkg==3.1.0",
        "beautifulsoup4==4.9.3",
        "cachetools==3.1.1",
        "Cerberus==1.2",
        "certifi==2019.11.28",
        "ciso8601==1.0.3",
        "decorator==4.1.2",
        "FormEncode==2.0.0a1",
        "googleads==31.0.0",
        "idna==2.8",
        "iso8601==0.1.4",
        "isodate==0.6.1",
        "jmespath==0.10.0",
        "Jinja2==2.10.1",
        "jsonpath-rw==1.4.0",
        "lxml==4.8.0",
        "MarkupSafe==1.1.1",
        "mock==3.0.5",
        "mysqlclient==1.3.14",
        "numpy==1.13.1",
        "pandas==0.23.4",
        "paramiko==2.0.9",
        "pycrypto==2.5",
        "pycryptodomex==3.9.8",
        "pylibmc==1.6.0",
        "pyOpenSSL==18.0.0",
        "pytz<2021.0",
        "python-dateutil==2.8.1",
        "requests==2.22.0",
        "rsa==3.4.2",
        "s3transfer==0.2.1",
        "snowflake-connector-python==2.1.3",
        "snowflake-sqlalchemy==1.1.18",
        "SQLAlchemy==1.1.4",
        "sqlparse==0.2.4",
        "zeep==3.4.0",
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
