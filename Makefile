VERSION= 1.01

install:
	install --mode=755 CatsOk.py /usr/local/bin
	install --mode=755 CatsOk /usr/local/bin
	install --mode=644 CatsOkScreenrc /usr/local/etc

dist:
	ln -fs . CatsOk-$(VERSION)
	tar czvf CatsOk-$(VERSION).tar.gz CatsOk-$(VERSION)/CatsOk.py CatsOk-$(VERSION)/CatsOk CatsOk-$(VERSION)/CatsOkScreenrc CatsOk-$(VERSION)/Makefile
	rm -f CatsOk-$(VERSION)
