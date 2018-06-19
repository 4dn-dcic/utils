import os
from setuptools import setup

# variables used in buildout
here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.md')).read()

requires = [
    'pytest-runner',
    'boto3',
    'elasticsearch>=5.3.0,<6.0.0',
    'elasticsearch-curator==5.*',
    'aws_requests_auth'
]

tests_require = [
    'pytest',
    'pytest-mock',
    'pytest-cov',
]

setup(
    name='dcicutils',
    version=open("dcicutils/_version.py").readlines()[-1].split()[-1].strip("\"'"),
    description='Utility modules shared amongst several repos in the 4dn-dcic organization',
    long_description=README,
    packages=['dcicutils'],
    include_package_data=True,
    zip_safe=False,
    author='$dn Team at Harvard Medical School',
    author_email='jeremy_johnson@hms.harvard.edu',
    url='https://data.4dnucleome.org',
    license='MIT',
    install_requires=requires,
    setup_requires=requires,
    tests_require=tests_require,
    extras_require={
        'test': tests_require,
    },
)
