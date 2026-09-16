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

import io
import re
from os import path
from setuptools import setup, find_packages

this_directory = path.abspath(path.dirname(__file__))


def read_version():
    """Read __version__ from the package without importing it.

    sqlalchemy_drill/__init__.py registers dialects with SQLAlchemy on import,
    so it cannot be imported before dependencies are installed.  The package is
    the single source of truth for the version; setup.py must not carry a
    second copy that can drift.
    """
    init_path = path.join(this_directory, 'sqlalchemy_drill', '__init__.py')
    with io.open(init_path, encoding='utf-8') as handle:
        for line in handle:
            match = re.match(
                r"""^__version__\s*=\s*['"]([^'"]+)['"]""", line)
            if match:
                return match.group(1)
    raise RuntimeError(f'no __version__ found in {init_path}')


VERSION = read_version()

with io.open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(name='sqlalchemy_drill',
      version=VERSION,
      description="Apache Drill for SQLAlchemy",
      long_description=long_description,
      long_description_content_type="text/markdown",
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Environment :: Console',
          'License :: OSI Approved :: MIT License',
          'Intended Audience :: Developers',
          'Programming Language :: Python',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8',
          'Programming Language :: Python :: 3.9',
          'Programming Language :: Python :: Implementation :: CPython',
          'Topic :: Database :: Front-Ends',
      ],
      install_requires=[
          "requests",
          "ijson",
          "sqlalchemy>=1.4"
      ],
      extras_require={
          "jdbc": ["JPype1", "JayDeBeApi"],
          "odbc": ["pyodbc"],
      },
      keywords='SQLAlchemy Apache Drill',
      author='John Omernik, Charles Givre, Davide Miceli, Massimo Martiradonna'
      ', James Turton',
      author_email='john@omernik.com, cgivre@thedataist.com, davide.miceli.dap'
      '@gmail.com, massimo.martiradonna.dap@gmail.com, james@somecomputer.xyz',
      license='MIT',
      url='https://github.com/JohnOmernik/sqlalchemy-drill',
      # Release tags carry a "v" prefix, so archive/<version>.tar.gz is a 404.
      download_url='https://github.com/JohnOmernik/sqlalchemy-drill/archive/'
      f'refs/tags/v{VERSION}.tar.gz',
      # "test" is a top-level directory with an __init__.py, so an unfiltered
      # find_packages() shipped it in the wheel, where it installed as a
      # top-level "test" package and shadowed CPython's stdlib test package.
      packages=find_packages(exclude=['test', 'test.*']),
      include_package_data=True,
      # tests_require/test_suite removed: setuptools dropped both, so they only
      # produced "Unknown distribution option" warnings on every invocation --
      # including "setup.py --version", which the packaging gate parses.
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
