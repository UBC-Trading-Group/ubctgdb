from setuptools import setup, find_packages

setup(
    name="ubctgdb",
    version="1.2.0",
    packages=find_packages(include=["ubctgdb", "ubctgdb.*"]),
    install_requires=[
        "boto3>=1.36,<2",
        "pandas>=2.0",
        "python-dotenv>=1.0",
        "filelock>=3.12,<4",
        "pyarrow>=10.0",     
    ],
    python_requires=">=3.9",
)
