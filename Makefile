download:
	- wget -nc https://storage.googleapis.com/sqlanywhere-driver/sqla17developerlinux.tar.gz

build:
	docker build -t sqlanywhere-driver-image ${PWD}

rebuild:
	docker build --no-cache -t sqlanywhere-driver-image ${PWD}

run:
	@echo "Running in the foreground. To test, for example run: 'go test --count 2 --timeout 24h -v"
	docker run -it --rm \
		--volume ${PWD}:/app \
		--name sqlanywhere-driver \
		sqlanywhere-driver-image		

clean:
	@echo "stop and remove containers, remove images. Inores errors if already stopped / removed."
	- docker stop	sqlanywhere-driver
	- docker rm		sqlanywhere-driver
	- docker rmi	sqlanywhere-driver-image

all: clean download rebuild run
