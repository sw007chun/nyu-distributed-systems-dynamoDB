defmodule Ping.Example do
  def ping do
    index = CHash.key_of(:os.timestamp())
    preflist = Ring.Manager.get_preference_list(index, 1)
    [index_node] = preflist
    Vnode.Master.sync_command(index_node, :ping)
  end

  def ring_status do
    {:ok, ring} = Ring.Manager.get_my_ring
    Ring.print_status(ring)
  end
end