from setuptools import setup, find_packages

setup(
    name="ubctgdb",
    version="0.2.0",
    packages=find_packages(include=["ubctgdb", "ubctgdb.*"]),
    install_requires=[
        "SQLAlchemy>=2.0",
        "pandas>=2.0",
        "diskcache>=5.0",
        "python-dotenv>=1.0",
        "mysqlclient>=2.0",  
        "PyMySQL>=1.0",       
        "pyarrow>=10.0",     
    ],
    python_requires=">=3.9",
)
