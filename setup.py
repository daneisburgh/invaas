"""
This file configures the Python package with entrypoints used for future runs on Databricks.

Please follow the `entry_points` documentation for more details on how to configure the entrypoint:
* https://setuptools.pypa.io/en/latest/userguide/entry_point.html
"""

from setuptools import find_packages, setup
from invaas import __version__

PACKAGE_REQUIREMENTS = [
    "pandas==2.1.3",
    "pandas-ta==0.3.14b",
    "playwright==1.41.0",
    "playwright-stealth==1.0.6",
    "python-dotenv==1.0.0",
    "yfinance==0.2.36",
]

DEV_REQUIREMENTS = [
    "black[jupyter]",
    "coverage[toml]",
    "delta-spark",
    "ipympl",
    "jupyter",
    "matplotlib",
    "mypy",
    "pandarallel",
    "pyspark",
    "pytest",
    "pytest-cov",
    "python-semantic-release",
    "scikit-learn",
    "setuptools",
    "wheel",
]

setup(
    name="invaas",
    packages=find_packages(exclude=["tests", "tests.*"]),
    setup_requires=["wheel"],
    install_requires=PACKAGE_REQUIREMENTS,
    extras_require={"dev": DEV_REQUIREMENTS},
    version=__version__,
    description="Investment as a Service",
    author="daneisburgh@gmail.com",
)
