REBAR=$(shell which rebar || echo ./rebar)
APP='mongodb'

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
test: clean all
	@$(REBAR) eunit app=$(APP)
		

