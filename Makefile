ERL=$(shell which erl)
REBAR=$(shell which rebar || echo ./rebar)
APP=mongodb
PA=./ebin ./deps/*/ebin

.PHONY: deps

all: deps compile

compile:
	@$(REBAR) compile
deps:
	@$(REBAR) get-deps
clean:
	@$(REBAR) clean
distclean: clean
	@$(REBAR) delete-deps
# Run a set of test for normal connection.
test: clean all
	@$(ERL) -pa $(PA) -eval 'mongodb_tests:test(), init:stop().' -noshell
# Run a set of tests for replica set.
testrs: clean all
	@$(ERL) -pa $(PA) -eval 'mongodb_tests:test_rs(), init:stop().' -noshell
doc: all
	@$(REBAR) doc
