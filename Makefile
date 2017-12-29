
PYTHON ?= python
PIP ?= pip
DOCS_SOURCE ?= docs
DOCS_BUILDDIR ?= build/docs

.PHONY: help

help:
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'

clean:		## clean build directory and cache files
	rm -fr dist/ *.eggs .eggs build/ pulsar/utils/*.so extensions/lib/clib.c
	find . -name '__pycache__' | xargs rm -rf
	find . -name '*.pyc' | xargs rm -rf
	find . -name '.DS_Store' | xargs rm -rf

compile:	## clean and build extension in place (for development)
	make clean
	$(PYTHON) setup.py build_ext -i

docs:		## build html documentation
	mkdir -p build/docs/html
	$(PYTHON) -m sphinx -a -b html $(DOCS_SOURCE) $(DOCS_BUILDDIR)/html

docs-spelling:	## check documentation spelling
	$(PYTHON) -m sphinx -a -b spelling $(DOCS_SOURCE) $(DOCS_BUILDDIR)/spelling

test:		## flake8 and unit-tests with uvloop
	flake8
	$(PYTHON) -W ignore setup.py test -q --io uv

testpy:		## pure python library unit tests (PULSARPY=yes)
	export PULSARPY=yes
	$(PYTHON) -W ignore setup.py test -q

coverage:	## run tunit tests with coverage
	export PULSARPY=yes; $(PYTHON) -W ignore setup.py test --coverage -q

testall:
	flake8
	$(PYTHON) -W ignore setup.py test -q
	$(PYTHON) -W ignore setup.py test -q --io uv
	$(PYTHON) setup.py bench

pypi-check:	## check if current version is valid for a new pypi release
	$(PYTHON) setup.py pypi --final

wheels:		## build platform wheels
	make clean
	$(PYTHON) setup.py bdist_wheel

wheels-mac:	## create wheels for Mac OSX **must be run from a mac**
	export PYMODULE=pulsar; export WHEEL=macosx; export CI=true; ./pulsar/cmds/build-wheels.sh

wheels-linux:	## create linux wheels for python 3.5 & 3.6
	rm -rf wheelhouse
	$(PYTHON) setup.py linux_wheels --py 3.5,3.6

wheels-test:	## run tests using wheels distribution
	rm -rf tmp
	mkdir tmp
	cp -r tests tmp/tests
	cp -r examples tmp/examples
	cp -r docs tmp/docs
	cp runtests.py tmp/runtests.py
	cd tmp && $(PYTHON) runtests.py
	rm -rf tmp

wheels-upload:	## upload wheels to s3
	$(PYTHON) setup.py s3data --bucket fluidily --key wheelhouse --files "wheelhouse/*.whl"

wheels-download:## download wheels from s3
	$(PYTHON) setup.py s3data --bucket fluidily --key wheelhouse --download

release: clean compile test
	$(PYTHON) setup.py sdist bdist_wheel upload
