
all:

clean:
	rm -rf var build
	rm -f cc/*.pyc cc/*/*.pyc
	rm -f debian/files debian/*.debhelper debian/*.log debian/*.substvars
	rm -f debian/files debian/pycompat
	rm -rf debian/cc debian/*-stampdir

deb:
	debuild -b -us -uc

lint:
	pylint -E --rcfile conf/pylint.cfg cc

test:
	python -m cc.test

