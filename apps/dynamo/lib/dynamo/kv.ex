defmodule KV do
  def ping do
    sync_command(:os.timestamp(), :ping)
  end

  def ring_status do
    {:ok, ring} = Ring.Manager.get_my_ring()
    Ring.print_status(ring)
  end

  @spec put(term(), term()) :: term()
  def put(key, value) do
    sync_command(key, {:put, key, value})
  end

  @spec get(term()) :: term()
  def get(key) do
    sync_command(key, {:get, key})
  end

  @spec delete(term()) :: term()
  def delete(key) do
    sync_command(key, {:delete, key})
  end

  @doc """
  Return key/value store of certain partition.
  """
  @spec get_my_data(non_neg_integer()) :: term()
  def get_my_data(partition) do
    Vnode.Master.sync_command({partition, Node.self()}, :get_my_data)
  end

  def sync_command(key, command) do
    index = CHash.key_of(key)
    preflist = Ring.Manager.get_preference_list(index, 1)
    [index_node] = preflist
    Vnode.Master.sync_command(index_node, command)
  end

  # Testing function
  def put_single(key, value, repl_index) do
    [{key_index, _}] = CHash.key_of(key) |> Ring.Manager.get_preference_list(1)
    # {:ok, ring} = Ring.Manager.get_my_ring()
    # index_as_int = CHash.next_index(index_as_int, ring.chring)
    preflist = Ring.Manager.get_preference_list(repl_index - 1, 1)
    [index_node] = preflist
    Vnode.Master.sync_command(index_node, {:put_single, key, value, key_index})
    # Vnode.Master.sync_command({index, Node.self()}, {:put_single, key, value, index_as_int})
  end

  # Remove after done
end
