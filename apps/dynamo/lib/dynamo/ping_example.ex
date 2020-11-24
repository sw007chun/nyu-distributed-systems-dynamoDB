defmodule Ping.Example do
  def ping do
    index = CHash.key_of(:os.timestamp())
    preflist = Ring.Manager.get_preference_list(index, 1)
    [index_node] = preflist
    Vnode.Master.sync_command(index_node, :ping)
  end
end