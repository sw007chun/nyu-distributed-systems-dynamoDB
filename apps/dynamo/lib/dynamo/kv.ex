defmodule KV do
  def ping do
    sync_command(:os.timestamp(), :ping)
  end

  def ring_status do
    {:ok, ring} = Ring.Manager.get_my_ring
    Ring.print_status(ring)
  end

  def put(key, value) do
    sync_command(key, {:put, key, value})
  end

  def get(key) do
    sync_command(key, {:get, key})
  end

  def delete(key) do
    sync_command(key, {:delete, key})
  end

  def sync_command(key, command) do
    index = CHash.key_of(key)
    preflist = Ring.Manager.get_preference_list(index, 1)
    [index_node] = preflist
    Vnode.Master.sync_command(index_node, command)
  end
end