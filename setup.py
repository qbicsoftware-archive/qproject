import os
from setuptools import setup
import versioneer

versioneer.VCS = 'git'
versioneer.versionfile_source = 'qproject/_version.py'
versioneer.versionfile_build = 'qproject/_version.py'
versioneer.tag_prefix = ''
versioneer.parentdir_prefix = 'qproject-'


def readme():
    dirname = os.path.dirname(__file__)
    with open(os.path.join(dirname, 'README.rst')) as f:
        return f.read()


setup(
    name="qproject",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    author="Adrian Seyboldt",
    author_email="adrian.seyboldt@web.de",
    description="Manage project directories and workflows at QBiC",
    license="GPL2+",
    packages=["qproject"],
    long_description=readme(),
    entry_points={
        'console_scripts': [
            'qproject = qproject.qproject:main'
        ]
    }
)
