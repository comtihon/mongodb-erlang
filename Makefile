PROJECT = mongodb

DIALYZER = dialyzer

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
tests: report-rebar3-version clean app eunit ct

eunit:
	./rebar3 eunit skip_deps=true

ct: app
	./rebar3 ct skip_deps=true

report-rebar3-version:
	@$(REBAR) version

# Dialyzer.
.$(PROJECT).plt: 
	@$(DIALYZER) --build_plt --output_plt .$(PROJECT).plt -r deps \
		--apps erts kernel stdlib sasl inets crypto public_key ssl mnesia syntax_tools asn1

clean-plt: 
	rm -f .$(PROJECT).plt

build-plt: clean-plt .$(PROJECT).plt

dialyze: .$(PROJECT).plt
	@$(DIALYZER) -I include -I deps --src -r src --plt .$(PROJECT).plt --no_native \
		-Werror_handling -Wrace_conditions -Wunmatched_returns

.PHONY: deps clean-plt build-plt dialyze report-rebar3-version
