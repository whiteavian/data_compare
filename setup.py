from distutils.core import setup

# Install with pip install .

setup(
    name='SQLDatabase',
    version='0.1dev',
    packages=['sql_database',],
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
    long_description=open('README.txt').read(),
    install_requires=[
        'sqlalchemy',
    ],
)