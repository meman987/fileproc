build:
	rm -rf dist
	rm -rf fileproc.egg-info 
	python -m build

dist:
	twine upload dist/*

install:
	python3 -m pip install ./dist/fileproc-2025.0.1.tar.gz
