defmodule Ping do
  use ExUnit.Case, async: true

  test "simple ping" do
    preflist = Ring.Manager.get_preference_list(<<0::160>>, 1)
    [index_node] = preflist
    assert {:pong, 182687704666362864775460604089535377456991567872} == Vnode.Master.sync_command(index_node, :ping)
  end
end