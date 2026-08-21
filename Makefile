-include .env

NF_HOST ?= 127.0.0.1
NF_PORT ?= 8888
NF_TIMEOUT ?= 3

export NF_HOST NF_PORT NF_TIMEOUT

.PHONY: configure build run test bench

configure:
	cmake --preset debug

build: configure
	cmake --build --preset debug --parallel

run: configure
	cmake --build --preset debug --target run

test: build
	ctest --preset debug --output-on-failure

bench: configure
	cmake --build --preset debug --target bench