VERSION= 1.4

install:
	mkdir -m 0755 -p /var/log/lscat
	install -m 0755 CatsOk.py /usr/local/bin
	install -m 0644 CatsOk.service /usr/lib/systemd/system
	systemctl enable CatsOk.service
