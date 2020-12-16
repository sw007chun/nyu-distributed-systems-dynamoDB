defmodule Vnode do
  @moduledoc """
  Actual methods for vnodes
  """

  use GenServer
  require Logger

  @type index_as_int() :: integer()
  # random delay for receiving put, get quorum response

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
    Registry.register(Registry.Vnode, partition, :vnode)
    # map for each replicated partitions
    data =
      partition
      |> Ring.Manager.get_replicated_indices()
      |> MapSet.new()

    replication = Application.get_env(:dynamo, :replication)
    read = Application.get_env(:dynamo, :R)
    write = Application.get_env(:dynamo, :W)
    write_delay = Application.get_env(:dynamo, :write_delay)
    ars_delay = Application.get_env(:dynamo, :ars_delay)
    read_repair = Application.get_env(:dynamo, :read_repair)
    aae = Application.get_env(:dynamo, :aae)

    {:ok,
     %{
       partition: partition,
       data: data,
       replication: replication,
       read: read,
       write: write,
       read_repair: read_repair,
       aae: aae,
       write_delay: write_delay,
       ars_delay: ars_delay
     }}
  end

  @impl true
  def handle_call({:reset_param, param_list}, _from, state) do
    state =
      param_list
      |> Enum.reduce(state,
      fn {key, value}, state0 -> %{state0 | key => value} end)

    {:reply, :ok, state}
  end

  # Put update the vclock and put (key, {value, context}) pair into the db.
  # Responde after getting W replicated reponses back.
  @impl true
  def handle_cast({:put, key, value, context, return_pid}, state) do
    context =
      if context == :no_context do
        {_, context} =
          state.partition
          |> Vnode.Master.get_partition_storage
          |> Agent.get(&Map.get(&1, key, {[], %{}}))
        context
      else
        context
      end

    context = Vclock.increment(context, Node.self())

    # Method for receiving put response from replication vnodes
    pref_list = Ring.Manager.get_preference_list(state.partition - 1, state.replication)

    # send asynchronous replication task to other vnodes
    for {index, node} <- pref_list do
      Dynamo.TaskSupervisor
      |> Task.Supervisor.start_child(fn ->
        Process.sleep(random_delay(state.write_delay))
        GenServer.cast(
          {Vnode.Master, node},
          {:command, index, {:put_repl, key, value, context, return_pid, false}}
        )
      end)
    end

    if state.aae do
      ActiveAntiEntropy.insert(key, value, state.partition)
    end

    {:noreply, state}
  end

  @impl true
  def handle_cast({:get, key, return_pid}, state) do
    pref_list =
      CHash.hash_of(key)
      |> Ring.Manager.get_preference_list(state.replication)

    [{partition_index, _} | _] = pref_list

    # send asynchronous replication task to other vnodes
    for {index, node} <- pref_list do
      Dynamo.TaskSupervisor
      |> Task.Supervisor.start_child(fn ->
        Process.sleep(random_delay(state.ars_delay))
        GenServer.cast(
          {Vnode.Master, node},
          {:command, index, {:get_repl, key, partition_index, return_pid}}
        )
      end)
    end
    {:noreply, state}
  end

  # Callback for put replication.
  @impl true
  def handle_cast({:put_repl, key, value, context, sender, read_repair?}, state) do
    index = Ring.Manager.key_partition_index(key)
    :ok =
      index
      |> Vnode.Master.get_partition_storage
      |> Agent.update(&Map.put(&1, key, {value, context}))

    if read_repair? do
      Logger.debug("#{Node.self()} read repairing #{key}: #{inspect value}")
    else
      Process.send_after(self(), {:reply, sender, :ok}, random_delay(state.ars_delay))
      Logger.debug("#{Node.self()} replicating #{key}: #{inspect value}")
    end

    if state.aae do
      ActiveAntiEntropy.insert(key, value, index)
    end

    {:noreply, state}
  end

  # Callback for get values from replicas
  @impl true
  def handle_cast({:get_repl, key, index, sender}, state) do
    {value, context} =
      index
      |> Vnode.Master.get_partition_storage
      |> Agent.get(&Map.get(&1, key, {[], %{}}))

    Process.send_after(self(), {:reply, sender, {:ok, {context, {value, self()}}}}, random_delay(state.ars_delay))
    Logger.info("#{Node.self()} returning #{key}: #{inspect value} stored in replica")

    {:noreply, state}
  end

  # This is to handle delayed reponses for the testing
  @impl true
  def handle_info({:reply, sender, reply}, state) do
    send(sender, reply)
    {:noreply, state}
  end
end
