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

ct:
	@$(REBAR) ct skip_deps=true suites=gen_process

# Dialyzer.
build-plt:
	@$(DIALYZER) --build_plt --output_plt .$(PROJECT).plt \
		--apps erts kernel stdlib sasl inets crypto public_key ssl mnesia syntax_tools

dialyze:
	@$(DIALYZER) --src src --plt .$(PROJECT).plt --no_native \
		-Werror_handling -Wrace_conditions -Wunmatched_returns

.PHONY: deps
