PROJECT = mongodb

DIALYZER = dialyzer
REBAR = $(shell which rebar || echo ./rebar)
ifeq ($(shell which coon), /usr/bin/coon)
  BUILD = $(shell echo coon build)
  DEPS = $(shell echo coon deps)
  EUNIT = $(shell coon eunit --define TEST)
  CT = $(shell coon ct --define TEST)
else
  BUILD = $(shell echo $(REBAR) compile)
  DEPS = $(shell echo $(REBAR) get-deps)
  EUNIT = $(shell echo "$(REBAR) compile && $(REBAR) eunit skip_deps=true")
  CT = $(shell echo "$(REBAR) compile && $(REBAR) ct skip_deps=true")
endif


all: app

# Application.

deps:
	@$(DEPS)

app:
	@$(BUILD)

clean:
	rm -rf ebin
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
	@$(EUNIT)

ct:
	@$(CT)

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
