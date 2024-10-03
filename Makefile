################################################################################
# Target: lint                                                                 #
################################################################################
# Please use golangci-lint with matching version, otherwise you might encounter errors.
# You can download at https://github.com/golangci/golangci-lint/releases/
# Check .github/wortkflows/test.yaml for the version used in GitHub actions.

ifeq ($(GOOS),windows)
GOLANGCI_LINT:=golangci-lint.exe
else
GOLANGCI_LINT:=golangci-lint
endif

.PHONY: lint
lint:
	$(GOLANGCI_LINT) run --timeout=20m


################################################################################
# Target: check-linter                                                         #
################################################################################
.SILENT: check-linter # Silence output other than the application run
.PHONY: check-linter
check-linter:
	$(RUN_BUILD_TOOLS) check-linter


################################################################################
# Target: modtidy                                                              #
################################################################################
.PHONY: modtidy
modtidy:
	go mod tidy

################################################################################
# Target: gen-proto                                                            #
################################################################################
PROTOC ?=protoc
PROTOC_VERSION = 25.4
PROTOBUF_SUITE_VERSION = 25.4
PROTOC_GEN_GO_VERSION = v1.32.0

PROTOC_GEN_GO_GRPC_VERSION = 1.3.0

PROTOS:=$(shell ls proto)
PROTO_PREFIX:=github.com/diagridio/go-etcd-cron

.PHONY: check-proto-version
check-proto-version: ## Checking the version of proto related tools
	@test "$(shell protoc --version)" = "libprotoc $(PROTOC_VERSION)" \
	|| { echo "please use protoc $(PROTOC_VERSION) (protobuf $(PROTOBUF_SUITE_VERSION)) to generate proto"; exit 1; }

	@test "$(shell protoc-gen-go-grpc --version)" = "protoc-gen-go-grpc $(PROTOC_GEN_GO_GRPC_VERSION)" \
	|| { echo "please use protoc-gen-go-grpc $(PROTOC_GEN_GO_GRPC_VERSION) to generate proto"; exit 1; }

	@test "$(shell protoc-gen-go --version 2>&1)" = "protoc-gen-go $(PROTOC_GEN_GO_VERSION)" \
	|| { echo "please use protoc-gen-go $(PROTOC_GEN_GO_VERSION) to generate proto"; exit 1; }

# Generate archive files for each binary
# $(1): the binary name to be archived
define genProtoc
.PHONY: gen-proto-$(1)
gen-proto-$(1):
	$(PROTOC) --go_out=. --go_opt=module=$(PROTO_PREFIX) --go-grpc_out=. --go-grpc_opt=require_unimplemented_servers=false,module=$(PROTO_PREFIX) ./proto/$(1)/*
endef

$(foreach ITEM,$(PROTOS),$(eval $(call genProtoc,$(ITEM))))

GEN_PROTOS:=$(foreach ITEM,$(PROTOS),gen-proto-$(ITEM))

.PHONY: gen-proto
gen-proto: check-proto-version $(GEN_PROTOS) modtidy

test:
	go test -count 1 -timeout 300s --race ./...
