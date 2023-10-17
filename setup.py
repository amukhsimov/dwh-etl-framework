from setuptools import setup, find_packages

VERSION = '0.1.0'
DESCRIPTION = 'amukhsimov-dwh-etl-framework'
LONG_DESCRIPTION = 'No description'

# Setting up
setup(
    # the name must match the folder name 'verysimplemodule'
    name="amukhsimov-dwh-etl-framework",
    version=VERSION,
    author="Akmal Mukhsimov",
    author_email="aka.mukhsimov@gmail.com",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    install_requires=[
        #'pyspark==3.4.1',
        'psycopg2-binary==2.9.7',
        'cx_Oracle==8.3.0',
        'pandas==2.1.0',
        'numpy==1.24.4',
        'apache-airflow==2.7.1',
        'python-dotenv==1.0.0',
    ],
    # needs to be installed along with your package. Eg: 'caer'
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    packages=find_packages('src'),
    package_dir={'': 'src'},
    python_requires=">=3.9,<3.10"
)
