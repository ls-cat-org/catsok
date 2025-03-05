VERSION= 1.4

.PHONY: build
build:
	@echo "TODO: Convert CatsOk to python3, use this Makefile rule to run 'pylint --errors-only'"

# Installs for Ubuntu 18.04, directory structure is unchanged in
# 22.04 and 24.04.
install:
	mkdir -m 0755 -p /var/log/lscat
	install -m 0755 CatsOk.py /usr/local/bin
	install -m 0644 catsok.service /etc/systemd/system
	systemctl enable catsok.service
