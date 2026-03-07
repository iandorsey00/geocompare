# -*- coding: utf-8 -*-

# Learn more: https://github.com/iandorsey00/geodata

from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

setup(
    name='geodata',
    version='0.2',
    description='A program for getting information about and comparing geographies',
    long_description=readme,
    long_description_content_type='text/markdown',
    author='Ian Dorsey',
    author_email='ian.dorsey@gmail.com',
    url='https://github.com/iandorsey00/geodata',
    license='MIT',
    packages=find_packages(exclude=('tests', 'docs')),
    install_requires=[
        'pandas',
        'numpy',
        'geopy',
        'rapidfuzz==0.2.0',
    ],
)
