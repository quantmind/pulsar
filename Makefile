.PHONY: _default clean compile docs test coverage release


PYTHON ?= python


_default: compile


clean:
	rm -rf dist/ *.egg-info *.eggs build/ pulsar/utils/*.so extensions/lib/lib.c
	find . -name '__pycache__' | xargs rm -rf


compile: clean
	$(PYTHON) setup.py build_ext -i


docs: compile
	cd docs && $(PYTHON) -m sphinx -a -b html . _build/html


test:
    flake8
	$(PYTHON) -W ignore setup.py test -q


coverage:
    flake8
	$(PYTHON) -W ignore setup.py test --coverage -q


testall:
    flake8
    $(PYTHON) -W ignore setup.py test -q
	$(PYTHON) -W ignore setup.py test -q --io uv
	$(PYTHON) setup.py bench
	$(PYTHON) -W ignore setup.py test --coverage --http-py-parser -q


release: clean compile test
	$(PYTHON) setup.py sdist bdist_wheel upload
