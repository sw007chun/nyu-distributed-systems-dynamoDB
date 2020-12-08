.PHONY: foo bar baz

foo:
	mix release foo && PORT=4040  _build/dev/rel/foo/bin/foo start_iex

bar:
	mix release bar && PORT=4041 _build/dev/rel/bar/bin/bar start_iex

baz:
	mix release baz && PORT=4042 _build/dev/rel/baz/bin/baz start_iex

start_foo:
	mix release foo && PORT=4040 _build/dev/rel/foo/bin/foo start

start_bar:
	mix release bar && PORT=4041 _build/dev/rel/bar/bin/bar start

start_baz:
	mix release baz && PORT=4042 _build/dev/rel/baz/bin/baz start
