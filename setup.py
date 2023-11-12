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
    "python-dotenv==1.0.0",
    "scikit-learn==1.3.2",
    "yfinance==0.2.31",
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
    "setuptools",
    "wheel",
]

setup(
    name="invaas",
    packages=find_packages("./invaas"),
    setup_requires=["wheel"],
    install_requires=PACKAGE_REQUIREMENTS,
    extras_require={"dev": DEV_REQUIREMENTS},
    version=__version__,
    description="Investment as a service",
    author="daneisburgh@gmail.com",
)
