from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()


setup(name='ost',
      packages=find_packages(),
      include_package_data=True,
      version='0.1',
      description='High-level functionality for the inventory, download '
                  'and pre-processing of Sentinel-1 data',
      long_description=long_description,
      long_description_content_type="text/markdown",
      install_requires=['numpy',
                        'tqdm',
                        'matplotlib',
                        'psycopg2',
                        'requests',
                        'fiona',
                        'shapely',
                        'rtree',
                        'descartes',
                        'pandas',
                        'geopandas'
                       ],
       classifiers=[
          'License :: OSI Approved :: MIT License',
          'Operating System :: OS independent',
          'Programming Language :: Python :: 3',
      ],
      url='https://github.com/ESA-PhiLab/OpenSarToolkit',
      author='Andreas Vollrath',
      author_email='andreas.vollrath[at]esa.int',
      license='MIT License',
      keywords=['Sentinel-1', 'ESA', 'SAR', 'radar', 'Earth Observation'],
      zip_safe=False)
