.PHONY: _default clean compile docs test testall coverage release


PYTHON ?= python


_default: compile


clean:
	rm -fr dist/ *.egg-info *.eggs build/ pulsar/utils/*.so extensions/lib/clib.c
	find . -name '__pycache__' | xargs rm -rf


compile: clean
	$(PYTHON) setup.py build_ext -i


docs:
	mkdir -p build/docs/html
	$(PYTHON) -m sphinx -a -b html docs/source build/docs/html


test:
	flake8
	$(PYTHON) -W ignore setup.py test -q --io uv

testpy:
	PULSARPY=yes
	$(PYTHON) -W ignore setup.py test -q


coverage:
	PULSARPY=yes
	$(PYTHON) -W ignore setup.py test --coverage -q


testall:
	flake8
	$(PYTHON) -W ignore setup.py test -q
	$(PYTHON) -W ignore setup.py test -q --io uv
	$(PYTHON) setup.py bench


release: clean compile test
	$(PYTHON) setup.py sdist bdist_wheel upload
