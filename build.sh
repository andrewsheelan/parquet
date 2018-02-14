#!/bin/bash -x

find . -name ".DS_Store" -depth -exec rm {} \;
rm -rf dist package.zip
mkdir -p dist

pip install -r requirements.txt -t ./dist/
cp *.py dist

pushd dist; zip -r -9 package.zip *; popd
mv dist/package.zip .

exit 0
