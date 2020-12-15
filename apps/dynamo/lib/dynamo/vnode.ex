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
    read_repair = Application.get_env(:dynamo, :read_repair)
    aae = Application.get_env(:dynamo, :aae)
    {:ok, %{partition: partition, data: data, replication: replication, read: read, write: write, read_repair: read_repair, aae: aae}}
  end

  # Put update the vclock and put (key, {value, context}) pair into the db.
  # Responde after getting W replicated reponses back.
  @impl true
  def handle_cast({:put, key, value, context, return_pid}, state) do
    Logger.info("#{Node.self()} put #{key}: #{value} to #{state.partition}")

    storage = Vnode.Master.get_partition_storage(state.partition)
    context =
      if context == :no_context do
        {_, context} = Agent.get(storage, &Map.get(&1, key, {[], %{}}))
        context
      else
        context
      end
    context = Vclock.increment(context, Node.self())

    # Method for receiving put response from replication vnodes
    Vnode.Replication.replicate_put(key, value, context, state, return_pid)
    if state.aae do
      ActiveAntiEntropy.insert(key, value, state.partition)
    end

    {:noreply, state}
  end

  @impl true
  def handle_cast({:get, key, return_pid}, state) do
    Vnode.Replication.get_reponses(key, state, return_pid)
    {:noreply, state}
  end

  # Callback for put replication.
  @impl true
  def handle_cast({:put_repl, key, value, context, sender, read_repair?}, state) do
    index = Ring.Manager.key_partition_index(key)

    storage = Vnode.Master.get_partition_storage(index)
    Process.sleep(random_delay(@mean))
    Agent.update(storage, &Map.put(&1, key, {value, context}))

    if state.aae do
      ActiveAntiEntropy.insert(key, value, index)
    end

    if read_repair? do
      Logger.debug("#{Node.self()} read repairing #{key}: #{value}")
    else
      Logger.debug("#{Node.self()} replicating #{key}: #{inspect value}")
      send(sender, :ok)
    end

    {:noreply, state}
  end

  # Callback for get values from replicas
  @impl true
  def handle_cast({:get_repl, sender, index, key, delay}, state) do
    storage = Vnode.Master.get_partition_storage(index)
    {value, context} = Agent.get(storage, &Map.get(&1, key, {[], %{}}))
    Logger.info("#{Node.self()} returning #{key}: #{value} stored in replica")

    if delay do
      Process.sleep(random_delay(@mean))
    end

    send(sender, {:ok, {context, {value, self()}}})
    {:noreply, state}
  end
end
