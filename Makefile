
all:

clean:
	rm -rf var build
	rm -f cc/*.pyc

deb:
	debuild -b -us -uc

