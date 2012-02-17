REBAR=$(shell which rebar || echo ./rebar)

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

