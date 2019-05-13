import os
import sys
import re

from setuptools import setup, find_packages

v = open(os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])), 'sqlalchemy_drill', '__init__.py'))
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
      install_requires=[
        "requests",
        "numpy",
        "pandas",
        "sqlalchemy"
      ],
      keywords='SQLAlchemy Apache Drill',
      author='John Omernik, Charles Givre, Davide Miceli, Massimo Martiradonna, Bhargava Vadlamani',
      author_email='john@omernik.com, cgivre@thedataist.com, davide.miceli.dap@gmail.com, massimo.martiradonna.dap@gmail.com,vadlamani1729@gmail.com',
      license='Apache',
      packages=find_packages(),
      include_package_data=True,
      tests_require=['nose >= 0.11'],
      test_suite="nose.collector",
      zip_safe=False,
      entry_points={
         'sqlalchemy.dialects': [
              'drill = sqlalchemy_drill.sadrill:DrillDialect_sadrill',
              'drill.sadrill = sqlalchemy_drill.sadrill:DrillDialect_sadrill',
              ]
        }
)
