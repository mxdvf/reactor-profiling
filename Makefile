.PHONY: install_node install_jobc node job clean

REACTOR_GIT     ?= https://github.com/mxdvf/reactor.git
REACTOR_BRANCH  ?= master
NCTRL_CRATE     ?= reactor_nctrl
JCTRL_CRATE     ?= reactor_jctrl

install_node:
	cargo install \
	    --git $(REACTOR_GIT) \
	    --branch $(REACTOR_BRANCH) \
			--locked \
	    $(NCTRL_CRATE)

install_jobc:
	cargo install \
	    --git $(REACTOR_GIT) \
	    --branch $(REACTOR_BRANCH) \
			--locked \
	    $(JCTRL_CRATE)

build:
	cargo build --release --target-dir target

node: build
	@echo "Killing process on port 3000 if any..."
	@lsof -ti :3000 | xargs --no-run-if-empty kill
	reactor_nctrl --port 3000 target/release

job:
	reactor_jctrl ./profile.toml

clean:
	cargo uninstall reactor_nctrl || true
	cargo uninstall reactor_jctrl || true
	cargo clean
