import os
import pathlib
from setuptools import setup, find_packages

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

def parse_requirements(file):
    return sorted(set(
        line.partition('#')[0].strip()
        for line in open(os.path.join(os.path.dirname(__file__), file))
    )
                  -set('')
                  )


setup(
    name='opensartoolkit',
    packages=find_packages(),
    include_package_data=True,
    version='0.12.1',
    description='High-level functionality for the inventory, download '
                'and pre-processing of Sentinel-1 data',
    install_requires=parse_requirements('requirements.txt'),
    url='https://github.com/ESA-PhiLab/OpenSarToolkit',
    author='Andreas Vollrath',
    author_email="opensarkit@gmail.com",
    license='MIT License',
    keywords=['Sentinel-1', 'ESA', 'SAR', 'Radar',
              'Earth Observation', 'Remote Sensing',
              'Synthetic Aperture Radar'],
    long_description=README,
    long_description_content_type="text/markdown",
    zip_safe=False,
    setup_requires=['pytest-runner'],
    tests_require=['pytest']
)
