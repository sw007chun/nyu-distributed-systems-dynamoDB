defmodule KV do
  require Logger

  def ping do
    sync_command(:os.timestamp(), :ping)
  end

  def ring_status do
    {:ok, ring} = Ring.Manager.get_my_ring()
    Ring.print_status(ring)
  end

  @doc """
  Put value to Dynamo DB.
  Value is represented as a list.
  """
  @spec put(term(), term()) :: term()
  def put(key, value) do
    sync_command(key, {:put, key, [value]})
  end

  @doc """
  Get command can be coordinated by any vnode.
  """
  @spec get(term()) :: term()
  def get(key) do
    random_vnode = Ring.Manager.get_random_vnode()
    Vnode.Master.sync_command(random_vnode, {:get, key})
  end

  @spec get_all(term()) :: term()
  def get_all(key) do
    sync_command(key, {:get_all, key})
  end

  # Time(msec) to get consistent result from all vnodes
  # after a concurrent write.
  @spec stabilize(term()) :: integer()
  def stabilize(key) do
    start = now()
    done = try_until_stable(key)
    done - start
  end

  # Recurse until every vnodes return the same value
  @spec stabilize(term()) :: integer()
  def try_until_stable(key) do
    _ = get(key)

    if get_all(key) != false do
      now()
    else
      try_until_stable(key)
    end
  end

  @doc """
  Return key/value store of certain partition.
  """
  @spec get_my_data(non_neg_integer()) :: %{}
  def get_my_data(partition) do
    partition
    |> Vnode.Master.get_partition_storage
    |> Agent.get(& &1)
  end

  # Send a sync command to 1st vnode in the preference list
  @spec sync_command(term(), term()) :: term()
  def sync_command(key, command) do
    index = CHash.hash_of(key)
    preflist = Ring.Manager.get_preference_list(index, 1)
    [index_node] = preflist
    Vnode.Master.sync_command(index_node, command)
  end

  # Testing function
  # Function for testing
  # This intentinally put a value into a vnode without replication
  # This is for making concurrent values in the cluster
  def put_single(key, value) do
    [{key_index, _}] = CHash.hash_of(key) |> Ring.Manager.get_preference_list(1)
    storage = Vnode.Master.get_partition_storage(key_index)
    {_, context} = Agent.get(storage, &Map.get(&1, key, {[], %{}}))
    context = Vclock.increment(context, Node.self())
    Agent.update(storage, &Map.put(&1, key, {[value], context}))

    Logger.debug("Putting single #{key}: #{value} at #{key_index}. New context: #{inspect(context)}")
    ActiveAntiEntropy.insert(key, [value], key_index)
    :ok
  end

  def now() do
    {mega, seconds, ms} = :os.timestamp()
    (mega * 1_000_000 + seconds) * 1000 + :erlang.round(ms / 1000)
  end
end
