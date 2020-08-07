# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
import re
from os import path
from setuptools import setup, find_packages, Extension

v = open(os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])), 'sqlalchemy_drill', '__init__.py'))
# VERSION = re.compile(r".*__version__ = '(.*?)'", re.S).match(v.read()).group(1)
v.close()


this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(name='sqlalchemy_drill',
      version='0.1',
      description="Apache Drill for SQLAlchemy",
      long_description=long_description,
      long_description_content_type="text/markdown",
      classifiers=[
          'Development Status :: 4 - Beta',
          'Environment :: Console',
          'License :: OSI Approved :: MIT License',
          'Intended Audience :: Developers',
          'Programming Language :: Python',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.3',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8',
          'Programming Language :: Python :: 3.9',
          'Programming Language :: Python :: Implementation :: CPython',
          'Topic :: Database :: Front-Ends',
      ],
      install_requires=[
          "requests",
          "numpy",
          "pandas",
          "sqlalchemy"
      ],
      extras_require={
          "jdbc": ["JPype1==0.6.3", "JayDeBeApi"],
          "odbc": ["pyodbc"],
      },
      keywords='SQLAlchemy Apache Drill',
      author='John Omernik, Charles Givre, Davide Miceli, Massimo Martiradonna',
      author_email='john@omernik.com, cgivre@thedataist.com, davide.miceli.dap@gmail.com, massimo.martiradonna.dap@gmail.com',
      license='MIT',
      url = 'https://github.com/JohnOmernik/sqlalchemy-drill',
      download_url = 'https://github.com/JohnOmernik/sqlalchemy-drill/archive/0.1.tar.gz',
      packages=find_packages(),
      include_package_data=True,
      tests_require=['nose >= 0.11'],
      test_suite="nose.collector",
      zip_safe=False,
      entry_points={
          'sqlalchemy.dialects': [
              'drill = sqlalchemy_drill.sadrill:DrillDialect_sadrill',
              'drill.sadrill = sqlalchemy_drill.sadrill:DrillDialect_sadrill',
              'drill.jdbc = sqlalchemy_drill.jdbc:DrillDialect_jdbc',
              'drill.odbc = sqlalchemy_drill.odbc:DrillDialect_odbc',
          ]
      }
    )
