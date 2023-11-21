========
Overview
========

test project

Installation
============

::

    pip install delta-hive-connector-utility


或是

::

    python setup.py install

Usage
=============

To use the project:

.. code-block:: python

    import delta_hive_connector_utility
    delta_hive_connector_utility()

CLI (Command-Line Interface) Usage
===================================
::

    delta_hive_connector_utility hello-world


Development
===========

建立 Python 虛擬環境
--------------------
::

    python -m venv venv
    source venv/bin/activate


install depandencies
---------------------
根據你的專案編輯 `requirements.txt`、`requirements-dev.txt` 的相依套件，並安裝套件

::

    pip install -r requirements.txt
    pip install -r requirements-dev.txt


pre-commit
-----------
::

    pre-commit install


執行 cli.py
------------
設定 `PYTHONPATH`

::

    export PYTHONPATH="$PWD/src"

執行 __main__.py 設定好的方式執行 `cli.py`

::

    python -m delta_hive_connector_utility hello-world

或是

::

    python src/delta_hive_connector_utility hello-world

執行 Unit Test
--------------
::

    pytest --cov=src --cov-report=term-missing -vv tests

使用 tox
--------

To run all the steps, you can execute the "tox" command. This command will perform the following steps::

    tox

.. list-table::
    :widths: 10 90
    :stub-columns: 1

    - - clean
      - remove build artifacts
    - - check
      - check coding style with flake8, isort
    - - docs
      - generate Sphinx HTML documentation, including API docs
    - - py38, py39, py310
      - run tests with the specified Python version
    - - report
      - generate coverage report with the specified Python version



You can also execute specific steps individually.

Unit Test
^^^^^^^^^

To run tests, execute the following command::

    tox -e py38

To run tests and generate coverage report, execute the following command::

    tox -e report

A code coverage report will be generated and saved as *htmlcov/index.html.*

Note, to combine the coverage data from all the tox environments run:

.. list-table::
    :widths: 10 90
    :stub-columns: 1

    - - Windows
      - ::

            set PYTEST_ADDOPTS=--cov-append
            tox

    - - Other
      - ::

            PYTEST_ADDOPTS=--cov-append tox


Check Coding Style
^^^^^^^^^^^^^^^^^^
To run check coding style, execute the following command::

    tox -e check

Build Documentation
^^^^^^^^^^^^^^^^^^^
To run build documentation, execute the following command::

    tox -e docs

A documentation will be generated and saved as dist/docs directory.
