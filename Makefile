.PHONY: _default clean compile docs test testall coverage release


PYTHON ?= python
PIP ?= pip
DOCS_SOURCE ?= docs
DOCS_BUILDDIR ?= build/docs

_default: compile


clean:
	rm -fr dist/ *.eggs .eggs build/ pulsar/utils/*.so extensions/lib/clib.c
	find . -name '__pycache__' | xargs rm -rf


compile: clean
	$(PYTHON) setup.py build_ext -i


docs:
	mkdir -p build/docs/html
	$(PYTHON) -m sphinx -a -b html $(DOCS_SOURCE) $(DOCS_BUILDDIR)/html

docs-spelling:
	$(PYTHON) -m sphinx -a -b spelling $(DOCS_SOURCE) $(DOCS_BUILDDIR)/spelling

test:
	flake8
	$(PYTHON) -W ignore setup.py test -q --io uv

testinstalled:
	$(PYTHON) -W ignore runtests.py

testpy:
	export PULSARPY=yes
	$(PYTHON) -W ignore setup.py test -q


coverage:
	export PULSARPY=yes; $(PYTHON) -W ignore setup.py test --coverage -q


testall:
	flake8
	$(PYTHON) -W ignore setup.py test -q
	$(PYTHON) -W ignore setup.py test -q --io uv
	$(PYTHON) setup.py bench

pypi-check:
	$(PYTHON) setup.py pypi --final

wheels:
	export PYMODULE=pulsar; export WHEEL=macosx; export CI=true; ./pulsar/cmds/build-wheels.sh

wheels-linux:
	rm -rf wheelhouse
	$(PYTHON) setup.py linux_wheels --py 3.5,3.6

wheels-upload:
	$(PYTHON) setup.py s3data --bucket fluidily --key wheelhouse --files "wheelhouse/*.whl"

wheels-download:
	$(PYTHON) setup.py s3data --bucket fluidily --key wheelhouse --download

release: clean compile test
	$(PYTHON) setup.py sdist bdist_wheel upload
