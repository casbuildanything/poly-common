.PHONY: proto clean

PROTO_DIR := proto
PB_DIR := pb

proto:
	@echo "Generating protobuf files..."
	@mkdir -p $(PB_DIR)
	protoc --proto_path=$(PROTO_DIR) \
		--go_out=$(PB_DIR) \
		--go_opt=paths=source_relative \
		$(PROTO_DIR)/*.proto
	@echo "Done!"

clean:
	@rm -rf $(PB_DIR)/*.pb.go

install-tools:
	@echo "Installing protoc-gen-go..."
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

