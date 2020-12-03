.PHONY: foo bar baz

foo:
	mix release foo && _build/dev/rel/foo/bin/foo start_iex

bar:
	mix release bar && _build/dev/rel/bar/bin/bar start_iex

baz:
	mix release baz && _build/dev/rel/baz/bin/baz start_iex

swchun:
	mix release swchun && _build/dev/rel/swchun/bin/swchun start_iex
