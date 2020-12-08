defmodule Ping do
  use ExUnit.Case, async: true

  test "simple ping" do
    preflist = Ring.Manager.get_preference_list(<<0::160>>, 1)
    [index_node] = preflist

    assert {:pong, 182_687_704_666_362_864_775_460_604_089_535_377_456_991_567_872} ==
             Vnode.Master.sync_command(index_node, :ping)
  end
end
