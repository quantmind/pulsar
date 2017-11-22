.PHONY: _default clean compile docs test testall coverage release


PYTHON ?= python
PIP ?= pip

_default: compile


clean:
	rm -fr dist/ *.egg-info *.eggs .eggs build/ pulsar/utils/*.so extensions/lib/clib.c
	find . -name '__pycache__' | xargs rm -rf


compile: clean
	$(PYTHON) setup.py build_ext -i


docs:
	$(PIP) install -r requirements/docs.txt
	mkdir -p build/docs/html
	$(PYTHON) -m sphinx -a -b html docs/source build/docs/html


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

linuxwheels:
	rm -rf wheelhouse
	$(PYTHON) setup.py linux_wheels --py 3.5,3.6

uploadwheels:
	$(PYTHON) setup.py s3data --bucket fluidily --key wheelhouse --files "wheelhouse/*.whl"

wheels:
	export PYMODULE=pulsar; export WHEEL=macosx; export CI=true; ./pulsar/cmds/build-wheels.sh

release: clean compile test
	$(PYTHON) setup.py sdist bdist_wheel upload
