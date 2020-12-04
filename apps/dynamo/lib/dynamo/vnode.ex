defmodule Vnode do
  @moduledoc """
  Actual methods for vnodes
  """

  use GenServer
  require Logger

  @type index_as_int() :: integer()

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

  def replicate(key, value) do
    GenServer.call(__MODULE__, {:repl, key, value})
  end

  # Replicate operations to following vnodes
  # This can be reused for put/get/delete operation
  defp replicate_task(key, value, my_index, num_replication, num_write) do
    pref_list = Ring.Manager.get_preference_list(my_index, num_replication - 1)
    pref_indices = MapSet.new(for {index, _node} <- pref_list, do: index)
    repair_indices = MapSet.new()
    current_write = 1

    # spawn a asynchronous task for receiving the ack from replicating vnodes
    task =
      Dynamo.TaskSupervisor
      |> Task.Supervisor.async(
        fn ->
          wait_write_response(key, value, pref_indices, repair_indices, current_write, num_write)
        end)

    Logger.info("Receiver Task: #{inspect(task)}")

    # send asynchronous replication task to other vnodes
    for {index, node} <- pref_list do
      {:ok, replicate_task_pid} = {Dynamo.TaskSupervisor, node}
      |> Task.Supervisor.start_child(Vnode.Master, :async_task, [index, {:repl, task.pid, my_index, key, value}])
      Logger.info("Replicate Task: #{inspect(replicate_task_pid)}")
    end

    {left_list, repair_indices} = Task.await(task)
    # TODO : Task for left_list and read repair
    # after W values have returned :ok from put replication operation
    # wait for other return for some time and do read repairs.
  end

  # this is for spawning async task for getting acks from other vnodes
  defp wait_write_response(key, value, pref_indices, repair_indices, current_write, num_write) do
    if current_write < num_write do
      receive do
        # We need to add vclock or nonce for checking
        {:ok, ^key, ^value, index} ->
          # correct return value
          pref_indices = MapSet.delete(pref_indices, index)
          wait_write_response(key, value, pref_indices, repair_indices, current_write + 1, num_write)
        {:ok, ^key, value, index} ->
          # correct key but wrong return value
          # needs read repair
          pref_indices = MapSet.delete(pref_indices, index)
          repair_indices = MapSet.put(repair_indices, index)
          wait_write_response(key, value, pref_indices, repair_indices, current_write, num_write)
        other ->
          Logger.info("#{inspect(other)}")
          wait_write_response(key, value, pref_indices, repair_indices, current_write, num_write)
          # error?
      end
    else
      {pref_indices, repair_indices}
    end
  end

  # Vnode keeps state partition, the index of partition it's in charge
  # and data, a key/value stores of indicies.
  # data is a map of %{index => %{key => value}}
  @impl true
  def init(partition) do
    # get indices of previous (n-1) vnodes for replication
    replicated_indices = Ring.Manager.get_replicated_indices(partition)
    data =
      Ring.Manager.get_replicated_indices(partition)
      |> Map.new(
        fn index -> {index, %{}}
      end)
    {:ok, %{:partition => partition, :data => data}}
  end

  @doc """
  Callback for put replication to vnodes.
  """
  @impl true
  def handle_cast({:repl, sender, index, key, value}, state) do
    Logger.info("replicating #{key}: #{value} to #{state.partition}")

    {_, new_data} =
      state.data
      |> Map.get_and_update(index, fn index_store ->
        {nil, Map.put(index_store, key, value)}
      end)

    send(sender, {:ok, key, value, state.partition})
    {:noreply, %{state | data: new_data}}
  end

  @impl true
  def handle_call(:ping, _from, state) do
    {:reply, {:pong, state.partition}, state}
  end

  @impl true
  def handle_call({:put, key, value}, _from, state) do
    Logger.info("put #{key}: #{value}")

    # store key/value to my parition's key/value store
    {_, new_data} =
      state.data
      |> Map.get_and_update(state.partition, fn index_store ->
        {nil, Map.put(index_store, key, value)}
      end)

    replicate_task(key, value, state.partition, 3, 2)
    {:reply, :ok, %{state | data: new_data}}
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    Logger.info("get #{key}")
    # TODO : Check R get values from other vnodes
    value =
      state.data
      |> Map.get(state.partition)
      |> Map.get(key, :key_not_found)
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
end