import os
from setuptools import find_packages, setup


def parse_requirements(file):
    return sorted(set(
        line.partition('#')[0].strip()
        for line in open(os.path.join(os.path.dirname(__file__), file))
    ) - set(''))


setup(
    name='ost',
    url='https://github.com/Scartography/OpenSarToolkit',
    author='Andreas Vollrath, Petr Sevcik',
    author_email='andreas.vollrath@esa.int, petr.sevcik@Eox.at',
    license='MIT License',
    keywords=['Sentinel-1', 'ESA', 'SAR', 'Radar',
              'Earth Observation', 'Remote Sensing',
              'Synthetic Aperture Radar'],
    packages=find_packages(),
    include_package_data=True,
    version='0.1',
    description='High-level functionality for the inventory, download '
                'and pre-processing of Sentinel-1 data',
    install_requires=parse_requirements("requirements.txt"),
    extras_require={
      'test': parse_requirements('requirements_test.txt')
    },

    zip_safe=False,
    setup_requires=['pytest-runner'],
    tests_require=['pytest']
    )
