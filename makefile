.PHONY: dev-mcstart

INNER_PORT=11211

dev-mcstart:
	docker run -d -p 33013:$(INNER_PORT) memcached
	docker run -d -p 33014:$(INNER_PORT) memcached
	docker run -d -p 33015:$(INNER_PORT) memcached
	docker run -d -p 33016:$(INNER_PORT) memcached

	sleep 5

dev-mcstop:
	docker ps -q --filter="ancestor=memcached" | xargs --no-run-if-empty docker stop

dev: dev-mcstop dev-mcstart
