PROJECT = mongodb

DIALYZER = dialyzer
REBAR = $(shell which rebar || echo ./rebar)

all: app

# Application.

deps:
	@$(REBAR) get-deps

app: deps
	@$(REBAR) compile

clean:
	@$(REBAR) clean
	rm -f test/*.beam
	rm -f erl_crash.dump

docs: clean-docs
	@$(REBAR) doc skip_deps=true

clean-docs:
	rm -f doc/*.css
	rm -f doc/*.html
	rm -f doc/*.png
	rm -f doc/edoc-info

# Tests.
tests: clean app eunit ct

eunit:
	@$(REBAR) eunit skip_deps=true

ct: app
	@$(REBAR) ct skip_deps=true

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

.PHONY: deps clean-plt build-plt dialyze
