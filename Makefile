PROJECT = mongodb

all: app

# Application.

deps:
	./rebar3 get-deps

app: deps
	./rebar3 compile

clean:
	./rebar3 clean
	rm -f test/*.beam
	rm -f erl_crash.dump

docs: clean-docs
	./rebar3 doc skip_deps=true

clean-docs:
	rm -f doc/*.css
	rm -f doc/*.html
	rm -f doc/*.png
	rm -f doc/edoc-info

# Tests.
tests: report-rebar3-version clean app
	./rebar3 ci_test

eunit:
	./rebar3 eunit skip_deps=true

ct: app
	./rebar3 ct skip_deps=true

coverage:
	./rebar3 ci_coverage

report-rebar3-version:
	./rebar3 version

dialyzer:
	./rebar3 ci_dialyzer

.PHONY: deps dialyzer report-rebar3-version coverage
