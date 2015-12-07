try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'name': 'digEntityMerger',
    'description': 'digEntityMerger',
    'author': 'Dipsy Kapoor',
    'url': 'https://github.com/usc-isi-i2/dig-entity-merger',
    'download_url': 'https://github.com/usc-isi-i2/dig-entity-merger',
    'author_email': 'dipsykapoor@gmail.com',
    'version': '0.1',
    'install_requires': ['nose2'],
    # these are the subdirs of the current directory that we care about
    'packages': ['digEntityMerger'],
    'scripts': [],
}

setup(**config)
