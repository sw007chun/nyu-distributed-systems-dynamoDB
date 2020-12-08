defmodule Vnode do
  @moduledoc """
  Actual methods for vnodes
  """

  use GenServer
  require Logger

  @type index_as_int() :: integer()
  # random delay for receiving put, get quorum response
  @mean 10

  @doc """
  Start Vnode for partition starting at index.
  """
  @spec start_vnode(index_as_int()) :: pid()
  def start_vnode(index) do
    Vnode.Manager.get_vnode_pid(index)
  end

  @spec start_link(index_as_int()) :: {:ok, pid()}
  def start_link(index) do
    GenServer.start_link(__MODULE__, index)
  end

  # Vnode keeps state partition, the index of partition it's in charge
  # and data, a key/value stores of indicies.
  # data is a map of %{index => %{key => value}}
  @impl true
  def init(partition) do
    # get indices of previous (n-1) vnodes for replication
    replicated_indices = Ring.Manager.get_replicated_indices(partition)

    # map for each replicated partitions
    data =
      partition
      |> Ring.Manager.get_replicated_indices
      |> Map.new(fn index -> {index, %{}} end)

    replication = Application.get_env(:dynamo, :replication)
    read = Application.get_env(:dynamo, :R)
    write = Application.get_env(:dynamo, :W)
    {:ok, %{partition: partition, data: data, replication: replication, read: read, write: write}}
  end

  @doc """
  Callback for put replication to vnodes.
  """
  @impl true
  def handle_cast({:put_repl, sender, index, key, value, context, nonce}, state) do
    Logger.info("#{Node.self()} replicating #{key}: #{value} to #{state.partition}")

    # {_, new_data} =
    #   state.data
    #   |> Map.get_and_update(index, fn index_store ->
    #     {nil, Map.put(index_store, key, {value, context})}
    #   end)

    state = put_in(state, [:data, index, key], {value, context})

    Process.sleep(random_delay(@mean))
    send(sender, {:ok, nonce})
    {:noreply, state}
  end

  @doc """
  Callback for get values from replicas
  """
  @impl true
  def handle_cast({:get_repl, sender, index, key, delay}, state) do
    Logger.info("Returning #{key} value stored in replica")

    value =
      state.data
      |> Map.get(index, %{})
      |> Map.get(key, :key_not_found)

    if delay do
      Process.sleep(random_delay(@mean))
    end
    send(sender, {:ok, key, value, state.partition, Node.self()})
    {:noreply, state}
  end

  @impl true
  def handle_cast({:update_repl, key, value, index, context}, state) do
    {_, new_data} =
      state.data
      |> Map.get_and_update(index, fn index_store ->
        {nil, Map.put(index_store, key, {value, context})}
      end)

    {:noreply, %{state | data: new_data}}
  end

  @impl true
  def handle_call(:ping, _from, state) do
    {:reply, {:pong, state.partition}, state}
  end

  @impl true
  def handle_call({:put, key, value}, _from, state) do
    Logger.info("#{Node.self()} put #{key}: #{value} to #{state.partition}")

    {_, context} =
      state.data
      |> Map.get(state.partition, %{})
      |> Map.get(key, {nil, %{}})
    context = VClock.increment(Node.self(), context)

    # store key/value to my parition's key/value store
    # {_, new_data} =
    #   state.data
    #   |> Map.get_and_update(state.partition,
    #   fn index_store ->
    #     {nil, Map.put(index_store, key, {value, context})}
    #   end)

    state = put_in(state, [:data, state.partition, key], {value, context})

    Vnode.Replication.replicate_put(key, value, context, state.partition, state.replication, state.read)
    {:reply, :ok, state}
    # {:reply, :ok, %{state | data: new_data}}
  end

  # Function for testing
  @impl true
  def handle_call({:put_single, key, value, index}, _from, state) do
    Logger.info("Putting single #{key}")

    key_value_map = Map.get(state.data, index, %{})
    {_, context} = Map.get(key_value_map, key, {nil, %{}})
    context = VClock.increment(Node.self(), context)
    IO.puts("New context: #{inspect(context)}")
    key_value_map = Map.put(key_value_map, key, {value, context})
    new_data = Map.put(state.data, index, key_value_map)

    {_, new_data} =
      state.data
      |> Map.get_and_update(index, fn index_store ->
        {nil, Map.put(index_store, key, {value, context})}
      end)

    {:reply, :ok, %{state | data: new_data}}
  end

  # Remove after done

  @impl true
  def handle_call({:get, key, get_all}, _from, state) do
    Logger.info("#{Node.self()} get #{key}")
    {value, context} =
      state.data
      |> Map.get(state.partition)
      |> Map.get(key, :key_not_found)

    Logger.info("Waiting for R replies")

    {value, state} =
      if get_all do
        result = Vnode.Replication.get_all_read(key, value, context, state.partition, state, state.replication, state.replication)
        IO.puts result
        {value, state}
      else
        Vnode.Replication.replicate_get(key, value, context, state.partition, state, state.replication, state.read)
      end

    {:reply, value, state}
  end

  @impl true
  def handle_call({:delete, key}, _from, state) do
    # TODO : quorum checking
    Logger.info("delete #{key}")
    new_data = Map.delete(state.data, key)
    {:reply, Map.get(state.data, key, :key_not_found), %{state | data: new_data}}
  end

  @impl true
  def handle_call(:get_my_data, _from, state) do
    {:reply, state.data, state}
  end

  @spec random_delay(number()) :: number()
  defp random_delay(mean) do
    if mean > 0 do
      Statistics.Distributions.Exponential.rand(1.0 / mean)
      |> Float.round()
      |> trunc
    else
      0
    end
  end
end
