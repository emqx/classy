BUILD_DIR := $(CURDIR)/_build

REBAR := rebar3

compile:
	$(REBAR) do compile, dialyzer, xref

.PHONY: all
all: compile test

.PHONY: clean
clean: distclean

.PHONY: distclean
distclean:
	@rm -rf _build erl_crash.dump rebar3.crashdump rebar.lock

.PHONY: xref
xref:
	$(REBAR) xref

.PHONY: eunit
eunit:
	$(REBAR) do eunit --verbose --cover, proper --cover

.PHONY: test
test: smoke-test prop-membership
	$(REBAR) cover -v

.PHONY: smoke-test
smoke-test: eunit
	$(REBAR) ct --name ct@127.0.0.1 --verbose --cover --readable false

.PHONY: prop-membership
prop-membership:
	$(REBAR) proper -c -m fuzz_membership -n 10 --max_size 50 --noshrink

.PHONY: coveralls
coveralls:
	@rebar3 as test coveralls send

##########################################################################################
# Concuerror
##########################################################################################

CONCUERROR := $(BUILD_DIR)/Concuerror/bin/concuerror
CONCUERROR_RUN := $(CONCUERROR) \
	--treat_as_normal shutdown --treat_as_normal normal --treat_as_normal intentional \
	--treat_as_normal optvar_set --treat_as_normal optvar_stopped --treat_as_normal optvar_retry \
	-x code -x code_server -x error_handler \
	--pa $(BUILD_DIR)/concuerror+test/lib/snabbkaffe/ebin \
	--pa $(BUILD_DIR)/concuerror+test/lib/optvar/ebin \
	--pa $(BUILD_DIR)/concuerror+test/lib/gproc/ebin \
	--pa $(BUILD_DIR)/concuerror+test/lib/classy/ebin

concuerror = $(CONCUERROR_RUN) -f $(BUILD_DIR)/concuerror+test/lib/classy/test/concuerror_tests.beam -t $(1) || \
	{ cat concuerror_report.txt; exit 1; }

.PHONY: concuerror_test
concuerror_test: $(CONCUERROR)
	rebar3 as concuerror eunit -m concuerror_tests
	#$(call concuerror,tab_open_test)

$(CONCUERROR):
	mkdir -p _build/
	cd _build && git clone https://github.com/parapluu/Concuerror.git
	$(MAKE) -C _build/Concuerror/
