import os
import sys
import re

from setuptools import setup, find_packages

def download_jdbc_driver():
    classpath = os.environ['CLASSPATH']


v = open(os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])), 'sqlalchemy_drill', '__init__.py'))
VERSION = re.compile(r".*__version__ = '(.*?)'", re.S).match(v.read()).group(1)
v.close()

if "--install_jdbc_driver" in sys.argv:
    download_jdbc_driver()
    sys.argv.remove("--install_jdbc_driver")


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
        "pandas"
      ],
      keywords='SQLAlchemy Apache Drill',
      author='John Omernik, Charles Givre, Davide Miceli, Massimo Martiradonna',
      author_email='john@omernik.com, cgivre@thedataist.com, davide.miceli.dap@gmail.com, massimo.martiradonna.dap@gmail.com',
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
