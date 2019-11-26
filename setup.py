import os
from setuptools import setup, find_packages

def parse_requirements(file):
    return sorted(set(
        line.partition('#')[0].strip()
        for line in open(os.path.join(os.path.dirname(__file__), file))
    ) -set(''))

setup(name='ost',
      packages=find_packages(),
      include_package_data=True,
      version='0.9.3',
      description='High-level functionality for the inventory, download '
                  'and pre-processing of Sentinel-1 data',
      install_requires=parse_requirements('requirements.txt'),
      url='https://github.com/ESA-PhiLab/OpenSarToolkit',
      author='Andreas Vollrath',
      author_email='andreas.vollrath[at]esa.int',
      license='MIT License',
      keywords=['Sentinel-1', 'ESA', 'SAR', 'Radar',
                'Earth Observation', 'Remote Sensing',
                'Synthetic Aperture Radar'],
      zip_safe=False)
