from setuptools import setup, find_packages

setup(
    name='keyvalchemy',
    version = '0.0.1',
    author='Chris Green',
    packages=find_packages(exclude=['test']),
    url='http://github.com/chrisgreen1993/keyvalchemy',
    install_requires=[
        'sqlalchemy'
    ]
)
