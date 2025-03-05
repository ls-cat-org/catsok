VERSION= 1.4

.PHONY: build
build:
	pylint --errors-only CatsOk.py

# Installs for Ubuntu 18.04, directory structure is unchanged in
# 22.04 and 24.04.
install:
	mkdir -m 0755 -p /var/log/lscat
	install -m 0755 CatsOk.py /usr/local/bin
	install -m 0644 CatsOk.service /usr/lib/systemd/user
	systemctl enable CatsOk.service
