from setuptools import setup, find_packages

setup(
    name="food_consumption_trends",
    version="0.1.0",
    author="William ZOUNON",
    author_email="williamzounon@gmail.com",
    description="Food consumption and nutritional trends with BigQueryML, Python, Pyspark, BigQuery/Postgres.",
    packages=find_packages(),
    package_dir={'': 'src'},
    install_requires=[
        "psycopg2"
        "psycopg2-binary"
        "SQLAlchemy"
        "python-dotenv"
        "setuptools"
    ],
)

