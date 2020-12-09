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
      |> Ring.Manager.get_replicated_indices()
      |> Map.new(fn index -> {index, %{}} end)

    replication = Application.get_env(:dynamo, :replication)
    read = Application.get_env(:dynamo, :R)
    write = Application.get_env(:dynamo, :W)
    {:ok, %{partition: partition, data: data, replication: replication, read: read, write: write}}
  end

  @doc """
  Put update the vclock and put (key, {value, context}) pair into the db.
  Responde after getting W replicated reponses back.
  """
  @impl true
  def handle_call({:put, key, value}, _from, state) do
    Logger.info("#{Node.self()} put #{key}: #{value} to #{state.partition}")

    {_, context} =
      state.data
      |> Map.get(state.partition, %{})
      |> Map.get(key, {nil, %{}})

    context = VClock.increment(context, Node.self())
    state = put_in(state, [:data, state.partition, key], {value, context})

    # Method for receiving put response from replication vnodes
    Vnode.Replication.replicate_put(
      key,
      value,
      context,
      state.partition,
      state.replication,
      state.read
    )

    {:reply, :ok, state}
  end

  @doc """
  Callback for put replication.
  """
  @impl true
  def handle_cast({:put_repl, sender, index, key, value, context, nonce}, state) do
    Logger.info("#{Node.self()} replicating #{key}: #{value} to #{state.partition}")

    state = put_in(state, [:data, index, key], {value, context})
    Process.sleep(random_delay(@mean))
    send(sender, {:ok, nonce})
    {:noreply, state}
  end

  @impl true
  def handle_call({:get, key, get_all}, _from, state) do
    {value, context} =
      state.data
      |> Map.get(state.partition, %{})
      |> Map.get(key, {nil, %{}})

    {value, state} =
      if get_all do
        # Testing code
        # Retrieves all current values from replication vnodes and checks consistency
        result = Vnode.Replication.get_all_read(key, value, context, state)
        Logger.info("All values: " <> Enum.join(result, ", "))
        if value == result do
          {true, state}
        else
          {false, state}
        end
      else
        # Spawn a async task for get reponses from vnodes
        # Unlike put replication, spawning a new process is needed for read repair
        task =
          Dynamo.TaskSupervisor
          |> Task.Supervisor.async(fn ->
            Vnode.Replication.replicate_get(key, value, context, state)
          end)
        Task.await(task, :infinity)
      end

    {:reply, value, state}
  end

  @doc """
  Callback for get values from replicas
  """
  @impl true
  def handle_cast({:get_repl, sender, index, key, nonce, delay}, state) do
    Logger.info("Returning #{key} value stored in replica")

    value_context =
      state.data
      |> Map.get(index, %{})
      |> Map.get(key, {nil, %{}})

    if delay do
      Process.sleep(random_delay(@mean))
    end

    send(sender, {:ok, nonce, key, value_context, state.partition, Node.self()})
    {:noreply, state}
  end

  @impl true
  def handle_cast({:read_repair, key, value, index, context}, state) do
    state = put_in(state, [:data, index, key], {value, context})
    {:noreply, state}
  end

  @impl true
  def handle_call(:ping, _from, state) do
    {:reply, {:pong, state.partition}, state}
  end

  # Function for testing
  # This intentinally put a value into a vnode without replication
  # This is for making concurrent values in the cluster
  @impl true
  def handle_call({:put_single, key, value, index}, _from, state) do
    {_, context} =
      state.data
      |> Map.get(index, %{})
      |> Map.get(key, {nil, %{}})
    context = VClock.increment(context, Node.self())
    Logger.info("Putting single #{key}: #{value}. New context: #{inspect(context)}")

    state = put_in(state, [:data, index, key], {value, context})

    {:reply, :ok, state}
  end

  # Remove after done

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
