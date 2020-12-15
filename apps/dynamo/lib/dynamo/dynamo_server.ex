defmodule DynamoServer do
  use GenServer
  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  @spec get_my_data(non_neg_integer()) :: %{}
  def get_my_data(partition) do
    partition
    |> Vnode.Master.get_partition_storage()
    |> Agent.get(& &1)
  end

  # Send an async command to 1st vnode in the preference list
  @spec command(term(), term()) :: term()
  def command(key, command) do
    index = CHash.hash_of(key)
    preflist = Ring.Manager.get_preference_list(index, 1)
    [index_node] = preflist
    Vnode.Master.command(index_node, command)
  end

  @impl true
  def init(:ok) do
    Logger.info("Dynamo server #{Node.self()} started.")
    {:ok, []}
  end

  @impl true
  def handle_cast({:get, key, return_pid}, state) do
    # TODO: This returns a node not vnode
    Ring.Manager.get_random_vnode()
    |> Vnode.Master.command({:get, key, return_pid})

    {:noreply, state}
  end

  @impl true
  def handle_cast({:put, key, value, context, return_pid}, state) do
    command(key, {:put, key, [value], context, return_pid})
    {:noreply, state}
  end

  @impl true
  def handle_call({:join, node}, _from, state) do
    Task.Supervisor.start_child(Dynamo.TaskSupervisor, fn -> Dynamo.join(node) end)
    {:reply, "Joining #{inspect(node)}", state}
  end

  @impl true
  def handle_call(:leave, _from, state) do
    Task.Supervisor.start_child(Dynamo.TaskSupervisor, fn -> Dynamo.leave() end)
    {:reply, "Leaving cluster", state}
  end

  @impl true
  def handle_call(:get_membership_info, _from, state) do
    {:ok, ring} = Ring.Manager.get_my_ring()
    {:reply, ring.chring, state}
  end

  @impl true
  def handle_call(:ring_status, _from, state) do
    {:ok, ring} = Ring.Manager.get_my_ring()
    {:reply, Ring.print_status(ring), state}
  end

  @impl true
  def handle_call(:start_aae, _from, state) do
    if Application.get_env(:dynamo, :aae) do
      {:reply, ActiveAntiEntropy.start(), state}
    else
      {:reply, "AAE disabled", state}
    end
  end

  # This is a test code for putting a key/value pair into a single node only
  # in order to create inconsistent data.
  @impl true
  def handle_call({:put_single, key, value}, _from, state) do
    [{key_index, _}] = CHash.hash_of(key) |> Ring.Manager.get_preference_list(1)
    storage = Vnode.Master.get_partition_storage(key_index)
    {_, context} = Agent.get(storage, &Map.get(&1, key, {[], %{}}))
    context = Vclock.increment(context, Node.self())
    Agent.update(storage, &Map.put(&1, key, {[value], context}))

    Logger.debug(
      "Putting single #{key}: #{value} at #{key_index}. New context: #{inspect(context)}"
    )

    if Application.get_env(:dynamo, :read_repair) do
      ActiveAntiEntropy.insert(key, [value], key_index)
    end

    {:reply, :ok, state}
  end
end
