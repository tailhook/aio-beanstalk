#!/usr/bin/env python

from distutils.core import setup

setup(name='aio-beanstalk',
      version='0.1',
      description='Asyncio-based client for beanstalkd task server',
      author='Paul Colomiets',
      author_email='paul@colomiets.name',
      url='http://github.com/tailhook/aio-beanstalk',
      packages=[
          'aiobeanstalk',
      ],
      classifiers=[
          'Development Status :: 4 - Beta',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.4',
      ],
      )
