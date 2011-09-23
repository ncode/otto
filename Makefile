PYTHON=`which python`
DESTDIR=/
BUILDIR=$(CURDIR)/debian/otto
PROJECT=imapsync
VERSION=0.2.0

all:
	@echo "make source - Create source package"
	@echo "make install - Install on local system"
	@echo "make buildrpm - Generate a rpm package"
	@echo "make builddeb - Generate a deb package"
	@echo "make clean - Get rid of scratch and byte files"

source:
	$(PYTHON) setup.py sdist $(COMPILE)

install:
	$(PYTHON) setup.py install --root $(DESTDIR) $(COMPILE)

buildrpm:
	$(PYTHON) setup.py bdist_rpm --post-install=rpm/postinstall --pre-uninstall=rpm/preuninstall

builddeb:
	$(PYTHON) setup.py --command-packages=stdeb.command sdist_dsc --extra-cfg-file=settings.cfg bdist_deb

clean:
	$(PYTHON) setup.py clean
	rm -rf build/ MANIFEST deb_dist/ dist/
	find . -name '*.pyc' -delete
