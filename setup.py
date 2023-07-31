import glob
import os
import time

from setuptools import setup, find_packages
from src.meta.pypackage_meta import __title__, __version__

with open('README.md', 'r', encoding='utf-8') as f:
    readme = f.read()

with open('package/requirements.txt', 'r', encoding='utf-8') as f:
    requires = f.read().splitlines()

if not os.path.isdir("package/setup"):
    os.mkdir("package/setup")

[os.remove(f) for f in glob.glob('package/setup/*.*')]

os.system("cd package&&pip download -r requirements.txt -d ./setup")

time.sleep(1)

setup(
    name=__title__,
    version=__version__,
    author='exem',
    author_email='exem@ex-em.com',
    description="Exam Data Analyzer",
    long_description=readme,
    packages=find_packages(exclude=['tests']),
    install_requires=requires,
    python_requires=">=3.8",
    package_dir={
        "resources": "resources",
        "sql": "sql"
    },
    package_data={
        "resources": [
            '*/*-prod.json',
            '*/*-dev.json',
            'drain/drain3.ini',
            'intermax_decoding/intermax_decryption.jar'
        ],
        "sql": [
            '*/*/*.txt',
            '*/*/*/*.txt',
            '*/*/*/*/*.txt'
        ],
        "": [
            '../export/sql_excel/sql/*.txt',
            '../package/setup/*',
            '../bin/*.sh',
            '../*.bat',
            '../docs/*.pptx'
        ]
    }
)
