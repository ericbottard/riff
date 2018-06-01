.PHONY: build clean dockerize test kubectl-apply
COMPONENT = streaming-gateway

GO_SOURCES = $(shell find cmd pkg -type f -name '*.go')
TAG ?= $(shell cat ../VERSION)

build: $(COMPONENT)

test:
	go test ./...

$(COMPONENT): $(GO_SOURCES) ../vendor
	go build cmd/$(COMPONENT).go

gen-mocks: $(GO_SOURCES)
	go get -u github.com/vektra/mockery/.../
	go generate ./...

clean:
	rm -f $(COMPONENT)

dockerize: .dockerize

.dockerize: $(GO_SOURCES) ../vendor ../Dockerfile ../.dockerignore
	docker build .. --build-arg COMPONENT=$(COMPONENT) -t projectriff/$(COMPONENT):$(TAG) && touch .dockerize

debug-dockerize: $(GO_SOURCES) ../vendor ../Dockerfile-debug ../.dockerignore
	docker build .. --build-arg COMPONENT=$(COMPONENT) -t projectriff/$(COMPONENT):$(TAG) -f ../Dockerfile-debug && rm .dockerize

../vendor: ../glide.lock
	cd .. && glide install -v --force

../glide.lock: ../glide.yaml
	cd .. && glide up -v --force

kubectl-apply:
	kubectl apply -n riff-system -f config/deployment.yaml
	kubectl apply -n riff-system -f config/service-nodeport.yaml
