VERSION= 1.3

install:
	install -p --mode=755 CatsOk.py /usr/local/bin

dist:
	ln -fs . CatsOk-$(VERSION)
	tar czvf CatsOk-$(VERSION).tar.gz CatsOk-$(VERSION)/CatsOk.py CatsOk-$(VERSION)/CatsOk CatsOk-$(VERSION)/CatsOkScreenrc CatsOk-$(VERSION)/Makefile
	rm -f CatsOk-$(VERSION)
