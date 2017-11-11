import os.path
from setuptools import setup, find_packages

__DIR__ = os.path.abspath(os.path.dirname(__file__))

setup(
    name = 'rawl',
    version = '0.1.0b5',
    description = 'An ugly raw SQL postgresql db layer',
    url = 'https://github.com/mikeshultz/rawl',
    author = 'Mike Shultz',
    author_email = 'mike@mikeshultz.com',
    classifiers = [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Database',
        'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    keywords = 'postgresql database bindings sql',
    packages = find_packages(exclude = ['build', 'dist']),
    package_data = {'': ['README.md']},
    install_requires = [
        'psycopg2>=2.7.3.2',
    ]
)