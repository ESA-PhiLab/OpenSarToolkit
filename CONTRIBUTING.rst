Introduction
------------

First off, thank you for considering contributing to Active Admin. It's people like you that make Active Admin such a great tool.
Following these guidelines helps to communicate that you respect the time of the developers managing and developing this open source project. In return, they should reciprocate that respect in addressing your issue, assessing changes, and helping you finalize your pull requests.

:code:`OpenSarToolkit` is an open source project and we love to receive contributions from our community — you! There are many ways to contribute, from writing tutorials or blog posts, improving the documentation, submitting bug reports and feature requests or writing code which can be incorporated into the lib itself.

.. warning:: 

  Please, don't use the issue tracker for **support questions**. Instead, check if `discussion channels <https://github.com/ESA-PhiLab/OpenSarToolkit/discussions>`__ can help with your issue. 

Ground Rules
------------

**Responsibilities**
#. Ensure cross-platform compatibility for every change that's accepted. Windows, Mac, Debian & Ubuntu Linux.
#. Ensure that code that goes into core meets all requirements in the commitizen checklist
#. Create issues for any major changes and enhancements that you wish to make. Discuss things transparently and get community feedback.
#. Don't add any classes to the codebase unless absolutely needed. Err on the side of using functions.
#. Keep feature versions as small as possible, preferably one new feature per version.
#. Be welcoming to newcomers and encourage diverse new contributors from all backgrounds. See our `Code of Conduct <https://github.com/ESA-PhiLab/OpenSarToolkit/blob/main/CODE_OF_CONDUCT.md>`__.

Your First Contribution
-----------------------

Working on your first Pull Request? You can learn how from this *free* series, `How to Contribute to an Open Source Project on GitHub <https://egghead.io/series/how-to-contribute-to-an-open-source-project-on-github>`__.

At this point, you're ready to make your changes! Feel free to ask for help; everyone is a beginner at first :smile_cat:! If a maintainer asks you to "rebase" your PR, they're saying that a lot of code has changed, and that you need to update your branch so it's easier to merge.

Getting started
---------------

Report a bug
^^^^^^^^^^^^
.. danger:: 

  If you find a security vulnerability, do NOT open an issue. Email opensarkit@gmail.com instead.


When filing an issue, make sure to answer the questions predifined in the issue template, it will help us reproduce the bug and elp you debuging it.

If you find yourself wishing for a feature that doesn't exist in :code:`OpenSarToolkit`, you are probably not alone. There are bound to be others out there with similar needs. Many of the features that :code:`OpenSarToolkit` has today have been added because our users saw the need. Open an issue on our issues list on GitHub which describes the feature you would like to see, why you need it, and how it should work.

development env
^^^^^^^^^^^^^^^

To install the development environment of the :code:`OpenSarToolkit` lib, create a new virtual environment: 

.. code-block:: console

  $ cd OpenSarToolkit
  $ python -m venv venv
  (venv)$ source venv/bin/activate
  
Once in the venv, you can install :code:`GDAL` (https://pypi.org/project/GDAL/) :code:`SNAP` (http://step.esa.int/main/download/) and :code:`ORFEO` (https://www.orfeo-toolbox.org/download/). then install the lib in development mode:

.. code-block:: console

  $ pip install -e .[dev]
  
This will install the :code:`pre-commit` hooks that will be run each time you commit to the repository.

.. note:: 

  You are not force to use en :code:`venv` to run :code:`ost` but make sure that your dependencies are compatible

pull request
^^^^^^^^^^^^
For something that is bigger than a one or two line fix

#. Create your own fork of the code
#. Do the changes in your fork
#. If you like the change and think the project could use it:

   * Be sure you have followed the code style for the project.
   * run the test suit by running in the root folder of the lib:
    
     .. code-block:: 
    
         python -m pytest
         
   * Send a pull request using the provided template
   
✨ Happy contribuing ! ✨
