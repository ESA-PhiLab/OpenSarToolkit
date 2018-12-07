from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()


setup(name='ostpy',
      include_package_data=True,
      version='0.1dev',
      description='High-level functionality for the inventory, download '
                  'and pre-processing of Sentinel-1 data',
      long_description=long_description,
      long_description_content_type="text/markdown",
      install_requires=['numpy',
                        'tqdm',
                        'descartes',
                        'matplotlib',
                        'psycopg2',
                        'requests',
                        'gdal',
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
      packages=find_packages(),
      keywords=['Sentinel-1', 'ESA', 'SAR', 'radar', 'Earth Observation'],
      zip_safe=False)
