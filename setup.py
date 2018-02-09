try:
    from setuptools import setup, find_packages
except ImportError:
    raise

# get __version__
exec(open('otudb/__init__.py').read())

setup(
    name='otu_db',
    description='Store results of 16S OTU clusters in database to track and compare and analyze.',
    version=__version__,
    author='Benjamin (cometsong)',
    license='Apache License 2.0',
    url='https://github.com/cometsong/otu_db/',
    author_email='benjamin at cometsong dot net',
    install_requires=[
        'attrs>=17.4.0',
        'clize>=4.0.3',
        'SQLAlchemy>=1.2.2',
        'PyMySQL>=0.8.0',
    ],
    tests_require=[
        'pytest>=3.4.0',
    ],
    packages=find_packages('src'),
    scripts=[
        'import_otu_data.py',
    ],
)
