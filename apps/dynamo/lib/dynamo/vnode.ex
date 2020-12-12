defmodule Vnode do
  @moduledoc """
  Actual methods for vnodes
  """

  use GenServer
  require Logger

  @type index_as_int() :: integer()
  # random delay for receiving put, get quorum response
  @mean 10

  @spec start_link(index_as_int()) :: {:ok, pid()}
  def start_link(index) do
    GenServer.start_link(__MODULE__, index)
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

  # Vnode keeps state partition, the index of partition it's in charge
  # and data, a key/value stores of indicies.
  # data is a map of %{index => %{key => value}}
  @impl true
  def init(partition) do
    Registry.register(Registry.Vnode, partition, Node.self())
    # map for each replicated partitions
    data =
      partition
      |> Ring.Manager.get_replicated_indices
      |> MapSet.new

    replication = Application.get_env(:dynamo, :replication)
    read = Application.get_env(:dynamo, :R)
    write = Application.get_env(:dynamo, :W)
    {:ok, %{partition: partition, data: data, replication: replication, read: read, write: write}}
  end

  @impl true
  def handle_call(:ping, _from, state) do
    {:reply, {:pong, state.partition}, state}
  end

  # Put update the vclock and put (key, {value, context}) pair into the db.
  # Responde after getting W replicated reponses back.
  @impl true
  def handle_call({:put, key, value}, _from, state) do
    Logger.info("#{Node.self()} put #{key}: #{value} to #{state.partition}")

    storage = Vnode.Master.get_partition_storage(state.partition)
    {_, context} = Agent.get(storage, &Map.get(&1, key, {[], %{}}))
    context = Vclock.increment(context, Node.self())
    Agent.update(storage, &Map.put(&1, key, {value, context}))

    # Method for receiving put response from replication vnodes
    Vnode.Replication.replicate_put(key, value, context, state)
    ActiveAntiEntropy.insert(key, value, state.partition)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    # storage = Vnode.Master.get_partition_storage(state.partition)
    # {value, context} = Agent.get(storage, &Map.get(&1, key, {[], %{}}))

    # Spawn task to wait for R reponses from other vnodes
    task =
      Dynamo.TaskSupervisor
      |> Task.Supervisor.async(fn ->
        Vnode.Replication.get_reponses(key, state)
      end)

    Process.sleep(random_delay(@mean))
    value = Task.await(task, :infinity)
    Logger.info("#{Node.self()} get #{key}: #{value}")

    {:reply, value, state}
  end

  # Testing code
  # Retrieves all current values from replication vnodes and checks consistency
  @impl true
  def handle_call({:get_all, key}, _from, state) do
    storage = Vnode.Master.get_partition_storage(state.partition)
    {value, context} = Agent.get(storage, &Map.get(&1, key, {[], %{}}))

    result = Vnode.Replication.get_all_read(key, value, context, state)
    Logger.debug("#{inspect value}, #{inspect result}")

    {:reply, value == result, state}
  end

  # Callback for put replication.
  @impl true
  def handle_cast({:put_repl, index, key, value, context, sender, nonce, read_repair?}, state) do
    storage = Vnode.Master.get_partition_storage(index)
    Agent.update(storage, &Map.put(&1, key, {value, context}))
    ActiveAntiEntropy.insert(key, value, index)

    if not read_repair? do
      Logger.debug("#{Node.self()} replicating #{key}: #{value}")
      Process.sleep(random_delay(@mean))
      send(sender, {:ok, nonce})
    else
      Logger.debug("#{Node.self()} read repairing #{key}: #{value}")
    end

    {:noreply, state}
  end

  # Callback for get values from replicas
  @impl true
  def handle_cast({:get_repl, sender, index, key, nonce, delay}, state) do
    storage = Vnode.Master.get_partition_storage(index)
    {value, context} = Agent.get(storage, &Map.get(&1, key, {[], %{}}))

    if delay do
      Process.sleep(random_delay(@mean))
    end

    Logger.info("Returning #{key}: #{value} stored in replica")
    # send(sender, {:ok, nonce, key, value_context, self()})
    send(sender, {:ok, nonce, {context, {value, self()}}})
    {:noreply, state}
  end
end
