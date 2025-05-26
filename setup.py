from setuptools import setup, find_packages

setup(
    name="aclimate_v3_historical_spatial_etl",
    version='v3.0.0',
    author="santiago123x",
    author_email="s.calderon@cgiar.com",
    description="ETL pipeline designed to download, extract, and prepare historical spatial data for integration into GeoServer.",
    url="https://github.com/CIAT-DAPA/aclimate_v3_historical_spatial_etl",
    download_url="https://github.com/CIAT-DAPA/aclimate_v3_historical_spatial_etl",
    packages=find_packages('src'),
    package_dir={'': 'src'},
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.10",
    install_requires=[
    ]
)