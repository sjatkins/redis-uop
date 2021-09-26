__author__ = 'samantha'
from setuptools import setup, find_packages

setup(name='redis_uop',
      version='0.1',
      description='redis uop database',
      author='Samantha Atkins',
      author_email='sjatkins@gmail.com',
      license='MIT',
      packages=find_packages(),
      install_requires = ['uop', 'redis', 'redis_uop'],
      zip_safe=False)
