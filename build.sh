#!/bin/bash -x

find . -name ".DS_Store" -depth -exec rm {} \;
rm -rf dist package.zip
mkdir -p dist

pip2 install -r requirements.txt -t ./dist/
cp *.py dist

pushd dist; zip -r package.zip *; popd
mv dist/package.zip .

exit 0
