#!/bin/bash
set -e -x

PIP=${PIP:-pip}
PYTHON=${PYTHON:-python}
IOPATH=${IOPATH:-$PWD}
DIST=${IOPATH}/dist/
WHEELRE=${PYMODULE}-*-${WHEEL}_*.whl

# Compile wheels
${PIP} install --upgrade pip wheel
${PIP} install --upgrade setuptools cython
${PIP} install -r ${IOPATH}/requirements/ci.txt
${PIP} install -r ${IOPATH}/requirements/hard.txt
make -C ${IOPATH} PYTHON=${PYTHON} compile
${PIP} wheel ${IOPATH} -w ${DIST}

if [ $BUNDLE_WHEEL ]
then
    echo "Bundle external shared libraries into the wheels"
    for whl in ${DIST}*.whl; do
        ${BUNDLE_WHEEL} repair $whl -w ${DIST}
    done
fi

echo "Cleanup non-$PYMODULE wheels"
find ${DIST} -maxdepth 1 -type f ! -name ${WHEELRE} -print0 | xargs -0 rm -rf
ls ${DIST}

echo
echo "Clear python cache"
find ${IOPATH} -type d -name __pycache__ -print0 | xargs -0 rm -rf

echo
echo -n "Test: "
${PIP} install ${PYMODULE} --no-index -f file://${DIST}
make -C ${IOPATH} PYTHON=${PYTHON} testinstalled

mkdir -p ${IOPATH}/wheelhouse
mv ${IOPATH}/dist/*.whl ${IOPATH}/wheelhouse/
make -C ${IOPATH} clean
