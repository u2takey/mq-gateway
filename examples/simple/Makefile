PROTOC :=protoc

gen:
	$(PROTOC) -I/usr/local/include -I. \
		-I$(GOPATH)/src/ \
		-I$(GOPATH)/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
		-I$(GOPATH)/src/github.com/gogo/protobuf/protobuf \
		--gogo_out=plugins=grpc:. \
		./simple.proto

	$(PROTOC) -I/usr/local/include -I. \
		-I$(GOPATH)/src \
		-I$(GOPATH)/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
		--mq-gateway_out=logtostderr=true:. \
		./simple.proto