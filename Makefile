-include .env

NF_HOST ?= 127.0.0.1
NF_PORT ?= 8888
NF_TIMEOUT ?= 3
ARGS ?=

export NF_HOST NF_PORT NF_TIMEOUT

.PHONY: configure-debug configure-release build run test bench

configure-debug:
	cmake --preset debug

configure-release:
	cmake --preset release

build: configure-debug
	cmake --build --preset debug --parallel -- --no-print-directory

run: configure-debug
	./build/debug/server $(ARGS)

test: build
	ctest --preset debug --output-on-failure

bench: configure-release
	cmake --build --preset release --target bench --parallel -- --no-print-directory