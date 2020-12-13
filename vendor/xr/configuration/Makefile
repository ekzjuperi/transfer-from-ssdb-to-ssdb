PRJ = xr
APP = configuration
BIN = $(APP)
BR = `git name-rev --name-only HEAD`
VER = `git describe --tags --abbrev=0`
COMMIT =`git rev-parse --short HEAD`
TIMESTM = `date -u '+%Y-%m-%d_%H:%M:%S%p'`
FORMAT = v$(VER)-$(COMMIT)-$(TIMESTM)
DOCTAG = $(VER)-$(BR)

# All docker containers
CNTS = `sudo docker ps -a -q`
# All untagged docker images
IMGS = `sudo docker images -f "dangling=true" -q`
# cfg image docker id
ID = `sudo docker images -q xrcfg`

.PHONY: info
info:
	make -v
	sudo docker version --format 'Client: {{ .Client.Version }} Server: {{ .Server.Version }}'
	godep version
	go version
	git describe --tags
	echo "namespace:"$(PRJ) "appname:"$(APP) "binary-name:"$(BIN) "version:"$(FORMAT)

.PHONY: clean
clean:
	if [ "$(CNTS)" != "" ] ; then \
		sudo docker rm -f $(CNTS) ; \
	fi
	if [ "$(IMGS)" != "" ] ; then \
		sudo docker rmi -f $(IMGS) ; \
	fi
	if [ "$(ID)" != "" ] ; then \
		sudo docker rmi -f xrcfg ; \
	fi

.PHONY: test
test:
	sudo docker-compose build --no-cache xrcfg
	sudo docker-compose run --rm xrcfg
