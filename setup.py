from setuptools import setup, find_packages

setup(
    name="ubctgdb",
    version="1.1.0",
    packages=find_packages(include=["ubctgdb", "ubctgdb.*"]),
    install_requires=[
        "boto3>=1.36,<2",
        "pandas>=2.0",
        "python-dotenv>=1.0",
        "pyarrow>=10.0",     
    ],
    python_requires=">=3.9",
)
