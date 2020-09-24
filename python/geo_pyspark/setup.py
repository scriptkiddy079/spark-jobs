from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))
# jars_relative_path = "geo_pyspark/jars"

setup(
    name='Trails',
    version='0.1',
    zip_safe=True,
    packages=find_packages(),
    python_requires='>=3.6',
    install_requires=['pyspark', 'findspark', 'pandas', 'geopandas', 'geo_pyspark'],

)
