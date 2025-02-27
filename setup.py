from setuptools import setup, find_packages
from os import environ

__VERSION__ = environ.get("VBUILD") or "2025.0.0"


setup(
    name='millegrilles_webscraper',
    version=__VERSION__,
    packages=find_packages(),
    url='https://github.com/dugrema/millegrilles.web_scraper.python',
    license='AFFERO',
    author='Mathieu Dugre',
    author_email='mathieu.dugre@mdugre.info',
    description='Web Scraper for MilleGrilles',
    install_requires=[
        'pytz>=2020.4',
        'aiohttp>=3.11.13,<4',
        'aiohttp-session==2.12.0'
    ]
)
