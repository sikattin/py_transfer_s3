# -*- coding: utf-8 -*-


from setuptools import setup, find_packages

requirement = ['s3_client', 'boto3', 'mylogger', 'my_utils']
with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='transfer_s3',
    version='1.0.1',
    description='Transfering the specified file/dir to S3',
    long_description=readme,
    author='Takeki Shikano',
    author_email='',
    url='',
    license=license,
    packages=find_packages(exclude=('tests', 'docs'))
)

