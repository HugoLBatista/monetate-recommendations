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
        "azure-storage-blob==2.1.0",
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
