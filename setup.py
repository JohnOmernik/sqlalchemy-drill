import os
import re

from setuptools import setup

v = open(os.path.join(os.path.dirname(__file__), 'sqlalchemy_drill', '__init__.py'))
VERSION = re.compile(r".*__version__ = '(.*?)'", re.S).match(v.read()).group(1)
v.close()

readme = os.path.join(os.path.dirname(__file__), 'README.md')


setup(name='sqlalchemy_drill',
      version=VERSION,
      description="Apache Drill for SQLAlchemy",
      long_description=open(readme).read(),
      classifiers=[
      'Development Status :: 3 - Alpha',
      'Environment :: Console',
      'Intended Audience :: Developers',
      'Programming Language :: Python',
      'Programming Language :: Python :: 3',
      'Programming Language :: Python :: Implementation :: CPython',
      'Topic :: Database :: Front-Ends',
      ],
      keywords='SQLAlchemy Apache Drill',
      author='John Omernik, Charles Givre',
      author_email='john@omernik.com, cgivre@thedataist.com',
      license='Apache',
      packages=['sqlalchemy_drill'],
      include_package_data=True,
      tests_require=['nose >= 0.11'],
      test_suite="nose.collector",
      zip_safe=False,
      entry_points={
         'sqlalchemy.dialects': [
              'drill = sqlalchemy_drill.pyodbc:DrillDialect_pyodbc',
              'drill.pyodbc = sqlalchemy_drill.pyodbc:DrillDialect_pyodbc',
              'drill.pydrill = sqlalchemy_drill.pydrill:DrillDialect_pydrill',
              ]
        }
)
