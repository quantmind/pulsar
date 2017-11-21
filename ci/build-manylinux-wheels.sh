#!/bin/bash
set -e -x

# Compile wheels
PYTHON="/opt/python/${PYTHON_VERSION}/bin/python"
PIP="/opt/python/${PYTHON_VERSION}/bin/pip"
${PIP} install --upgrade pip wheel
${PIP} install --upgrade setuptools
${PIP} install -r /io/requirements/ci.txt
${PIP} install -r /io/requirements/hard.txt
make -C /io/ PYTHON="${PYTHON}" compile
${PIP} wheel /io/ -w /io/dist/

# Bundle external shared libraries into the wheels.
for whl in /io/dist/*.whl; do
    auditwheel repair $whl -w /io/dist/
done

echo "Cleanup OS specific wheels"
rm -fv /io/dist/*-linux_*.whl
echo "Cleanup non-$PYMODULE wheels"
find /io/dist -maxdepth 1 -type f ! -name "$PYMODULE"'-*-manylinux1_*.whl' -print0 | xargs -0 rm -rf
ls /io/dist


# clear python cache
find /io -type d -name __pycache__ -print0 | xargs -0 rm -rf

echo
echo -n "Test $PYTHON_VERSION: "
${PIP} install ${PYMODULE} --no-index -f file:///io/dist
make -C /io/ PYTHON="${PYTHON}" testinstalled

mkdir -p /io/wheelhouse
mv /io/dist/*-manylinux*.whl /io/wheelhouse/
