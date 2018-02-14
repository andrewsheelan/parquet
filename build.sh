#!/bin/bash -x

find . -name ".DS_Store" -depth -exec rm {} \;
rm -rf dist package.zip
mkdir -p dist

pipenv install
cp -r `pipenv --venv`/lib/python2.7/site-packages/* dist/
cp *.py dist

pushd dist; zip -r -9 package.zip *; popd
mv dist/package.zip .

exit 0
