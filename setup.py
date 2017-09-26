from setuptools import setup

setup(
    name='SQLDatabase',
    version='0.1dev',
    py_modules=['data_compare',],
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
    long_description=open('README.txt').read(),
    install_requires=[
        'sqlalchemy',
        'sqlalchemy-utils',
    ],
)
