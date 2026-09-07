SHELL := bash
.ONESHELL:
.SUFFIXES:
.SHELLFLAGS := -eu -o pipefail -c
MAKEFLAGS += --no-builtin-rules

ifeq ($(VERSION),)
	VERSION:=$(shell cat VERSION)
endif

CLJ_FILES=$(shell find src -name '*.clj' -o -name '*.cljc')
EDN_FILES=$(wildcard *.edn)

JAR_DEPS=$(EDN_FILES) $(CLJ_FILES)

TARGET_DIR:=target
OUTPUT_JAR=$(TARGET_DIR)/com.joshuadavey/astrolabe-$(VERSION).jar

.PHONY: clean deploy test

.DEFAULT_GOAL:=jar

test:
	bin/kaocha

jar: $(OUTPUT_JAR)

$(OUTPUT_JAR): $(JAR_DEPS)
	clojure -T:build build-jar

deploy: $(OUTPUT_JAR)
	clojure -T:build deploy

clean:
	@rm -rf $(TARGET_DIR)
