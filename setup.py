from pathlib import Path
import setuptools
from setuptools.command.develop import develop
from subprocess import check_call

# the version number
version = "0.12.15"

# The directory containing this file
HERE = Path(__file__).parent

DESCRIPTION = "High-level functionality for the inventory, download and pre-processing of Sentinel-1 data"
LONG_DESCRIPTION = (HERE / "README.rst").read_text()

dev_requires = [
    "pytest",
    "coverage",
    "flake8",
    "ipdb",
]


class DevelopCmd(develop):
    """overwrite normal develop pip command to install the pre-commit"""

    def run(self):
        check_call(
            [
                "pre-commit",
                "install",
                "--install-hooks",
                "-t",
                "pre-commit",
                "-t",
                "commit-msg",
            ]
        )
        super(DevelopCmd, self).run()

def get_requirements(source):
    with open('requirements.txt') as f:
        requirements = f.read().splitlines()

    required = []
    # do not add to required lines pointing to git repositories
    EGG_MARK = '#egg='
    for line in requirements:
        if line.startswith('-e git:') or line.startswith('-e git+') or \
                line.startswith('git:') or line.startswith('git+'):
            if EGG_MARK in line:
                package_name = line[line.find(EGG_MARK) + len(EGG_MARK):]
                required.append(f'{package_name} @ {line}')
            else:
                print('Dependency to a git repository should have the format:')
                print('git+ssh://git@github.com/xxxxx/xxxxxx#egg=package_name')
        else:
            required.append(line)

    return required

setuptools.setup(
    name="opensartoolkit",
    version=version,
    license="MIT License",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/x-rst",
    author="Andreas Vollrath",
    author_email="opensarkit@gmail.com",
    url="https://github.com/ESA-PhiLab/OpenSarToolkit",
    download_url=f"https://github.com/ESA-PhiLab/OpenSarToolkit/archive/{version}.tar.gz",
    keywords=[
        "Sentinel-1",
        "ESA",
        "SAR",
        "Radar",
        "Earth Observation",
        "Remote Sensing",
        "Synthetic Aperture Radar",
    ],
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=get_requirements('requirements.txt'),
    extras_require={
        "dev": dev_requires
    },
    #cmdclass={"develop": DevelopCmd},
    zip_safe=False,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Scientific/Engineering :: GIS",
    ],
)
