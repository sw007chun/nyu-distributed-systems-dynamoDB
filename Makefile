.PHONY: foo bar baz

foo:
	mix release foo && _build/dev/rel/foo/bin/foo start_iex

bar:
	mix release bar && _build/dev/rel/bar/bin/bar start_iex

baz:
	mix release baz && _build/dev/rel/baz/bin/baz start_iex

panda:
	mix release panda && _build/dev/rel/panda/bin/panda start_iex

client:
	mix release client && _build/dev/rel/client/bin/client start_iex

start_foo:
	mix release foo && _build/dev/rel/foo/bin/foo start

start_bar:
	mix release bar && _build/dev/rel/bar/bin/bar start

start_baz:
	mix release baz && _build/dev/rel/baz/bin/baz start

start_panda:
	mix release panda && _build/dev/rel/panda/bin/panda start

start_client:
	mix release client && _build/dev/rel/client/bin/client start
