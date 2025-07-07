from setuptools import setup, find_packages

setup(
    name="ubctgdb",
    version="0.1.1",
    packages=find_packages(),
    install_requires=[
        "SQLAlchemy>=2.0",
        "pandas>=2.0",
        "diskcache>=5.0",
        "python-dotenv>=1.0",
        "tqdm>=4.0",
        "mysqlclient>=2.0",
        "pymysql>=1.0",
        "pyarrow>=10.0",     
        "diskcache>=5.0",
        "pandas>=2.0",
        "diskcache>=5.0",
        "tqdm>=4.0",
    ],
    python_requires=">=3.9",
)
