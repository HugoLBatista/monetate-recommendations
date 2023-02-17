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
        "chardet<3.1.0,>=3.0.2",  # is required by requests
        "css-parser",
        "Django>=1.11,<2.0",
        "django-json-rpc",
        "djangorestframework<=3.9.0",  # last version which supports both py2 and py3
        "monetate-bandit",
        "monetate-recs<=1.3.6",
        "monetate-monitoring",
        "monetate-caching",
        "aws-encryption-sdk",
        "beautifulsoup4<=4.9.3",  # The final version of Beautiful Soup to support Python 2 was 4.9.3
        "cachetools<=3.1.1",  # last version which supports both py2 and py3
        "Cerberus",
        "certifi<2021.0.0",  # is required by snowflake-connector-python
        "ciso8601<=2.2.0",  # later versions stopped supporting py2
        "decorator<=4.1.2",  # last version which supports both py2 and py3
        "FormEncode",
        "idna<2.9",  # is required by snowflake-connector-python, requests
        "iso8601<=0.1.4",  # required by py2
        "isodate",
        "jmespath<1.0.0,>=0.7.1",  # is required by boto3, botocore
        "Jinja2~=2.10.1",  # is required by Markupsafe
        "jsonpath-rw",
        # newer versions of markupdafe ask for another version of requests which isn't compatible with python-snowflake-connector
        "MarkupSafe<=1.1.1",
        "mock<=3.0.5",
        "mysqlclient~=1.3.14",  # last version which supports both py2 and py3
        "pandas<=0.23.4",  # last version which supports both py2 and py3
        "paramiko<=2.0.9",  # last version which supports both py2 and py3
        "pycrypto",
        "pylibmc",
        "pyOpenSSL<=18.0.0",  # dependency from sqlalchemy
        "pytz<2021.0",  # strict requirement from snowflake-connector-python
        "python-dateutil==2.8.0",
        "requests==2.22.0",  # required by snowflake-connector to get the tests running
        "rsa<4.6",  # is required by google-auth
        "s3transfer<0.3.0,>=0.2.0",  # set by boto3
        "snowflake-connector-python==2.1.3",  # 2.7.3 was failing due to pyarrow dep
        "snowflake-sqlalchemy~=1.1.14",  # dependent on snowflake-connector-python
        "SQLAlchemy~=1.1.4",  # dependent on snowflake-connector-python
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
