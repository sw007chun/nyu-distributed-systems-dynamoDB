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
    GenServer.call(__MODULE__, {:put_repl, key, value})
  end

  # Replicate operations to following vnodes
  # This can be reused for put/get/delete operation
  defp replicate_put(key, value, context, my_index, num_replication, num_write) do
    pref_list = Ring.Manager.get_preference_list(my_index, num_replication - 1)
    pref_indices = MapSet.new(for {index, _node} <- pref_list, do: index)
    repair_indices = MapSet.new()

    # spawn a asynchronous task for receiving the ack from replicating vnodes
    task =
      Dynamo.TaskSupervisor
      |> Task.Supervisor.async(fn ->
        wait_write_response(1, num_write)
      end)

    # send asynchronous replication task to other vnodes
    for {index, node} <- pref_list do
      {:ok, replicate_task_pid} =
        {Dynamo.TaskSupervisor, node}
        |> Task.Supervisor.start_child(Vnode.Master, :async_task, [
          index,
          {:put_repl, task.pid, my_index, key, value, context}
        ])
    end

    Task.await(task)
  end

  defp replicate_get(key, value, context, my_index, state, num_replication, num_read) do
    pref_list = Ring.Manager.get_preference_list(my_index, num_replication - 1)
    current_read = 1

    # spawn a asynchronous task for receiving the ack from replicating vnodes
    task =
      Dynamo.TaskSupervisor
      |> Task.Supervisor.async(fn ->
        wait_read_response(key, value, my_index, context, state, current_read, num_read)
      end)

    # send asynchronous replication task to other vnodes
    for {index, node} <- pref_list do
      {:ok, replicate_task_pid} =
        {Dynamo.TaskSupervisor, node}
        |> Task.Supervisor.start_child(Vnode.Master, :async_task, [
          index,
          {:get_repl, task.pid, my_index, key}
        ])
    end

    Task.await(task)
  end

  # this is for spawning async task for getting acks from other vnodes
  defp wait_write_response(current_write, num_write) do
    if current_write < num_write do
      receive do
        # We need to add vclock or nonce for checking
        :ok ->
          wait_write_response(current_write + 1, num_write)

        other ->
          Logger.info("#{inspect(other)}")
          wait_write_response(current_write, num_write)
      end
    else
      :ok
    end
  end

  defp wait_read_response(key, value, index, context, state, current_read, num_read) do
    if current_read < num_read do
      receive do
        {:ok, ^key, {other_value, other_context}, other_index, sender} ->
          case VClock.compare_vclocks(context, other_context) do
            :after ->
              Logger.info("vclock received. Result of comparison: After")
              Logger.info("#{inspect(context)},...,#{inspect(other_context)}")

              GenServer.cast(
                {Vnode.Master, sender},
                {:command, Node.self(), other_index, {:update_repl, key, value, index, context}}
              )

              wait_read_response(key, value, index, context, state, current_read + 1, num_read)

            :before ->
              Logger.info("vclock received. Result of comparison: Before")
              Logger.info("#{inspect(context)},...,#{inspect(other_context)}")

              {_, index_map} =
                state.data
                |> Map.get_and_update(index, fn index_store ->
                  {nil, Map.put(index_store, key, {other_value, other_context})}
                end)

              state = %{state | data: index_map}

              wait_read_response(
                key,
                other_value,
                index,
                other_context,
                state,
                current_read + 1,
                num_read
              )

            :equal when value == other_value ->
              Logger.info("vclock received. Result of comparison: Equal")
              Logger.info("#{inspect(context)},...,#{inspect(other_context)}")
              wait_read_response(key, value, index, context, state, current_read + 1, num_read)

            _ ->
              Logger.info("vclock received. Result of comparison: Concurrent")
              Logger.info("#{inspect(context)},...,#{inspect(other_context)}")
              # Merging concurrent vclocks into one
              # Incrementing the vclock and adding all concurrent values to a list
              # Sending the update replica command back to the node
              new_context = VClock.merge_vclocks(context, other_context)
              new_context = VClock.increment(Node.self(), new_context)

              new_value =
                if is_list(value) do
                  value
                else
                  [value]
                end

              new_value =
                if is_list(other_value) do
                  new_value ++ other_value
                else
                  new_value ++ [other_value]
                end

              {_, index_map} =
                state.data
                |> Map.get_and_update(index, fn index_store ->
                  {nil, Map.put(index_store, key, {new_value, new_context})}
                end)

              state = %{state | data: index_map}

              GenServer.cast(
                {Vnode.Master, sender},
                {:command, Node.self(), other_index,
                 {:update_repl, key, new_value, index, new_context}}
              )

              wait_read_response(
                key,
                new_value,
                index,
                new_context,
                state,
                current_read + 1,
                num_read
              )
          end
      after
        10_000 ->
          Logger.info("Timed out while waiting for R replies")
          {value, state}
      end
    else
      {value, state}
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
  def handle_cast({:put_repl, sender, index, key, value, context}, state) do
    Logger.info("replicating #{key}: #{value} to #{state.partition}")

    {_, new_data} =
      state.data
      |> Map.get_and_update(index, fn index_store ->
        {nil, Map.put(index_store, key, {value, context})}
      end)

    send(sender, :ok)
    {:noreply, %{state | data: new_data}}
  end

  @doc """
  Callback for get values from replicas
  """
  @impl true
  def handle_cast({:get_repl, sender, index, key}, state) do
    Logger.info("returning #{key} value stored in replica")

    value =
      state.data
      |> Map.get(index, %{})
      |> Map.get(key, :key_not_found)

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
    Logger.info("put #{key}: #{value} to #{state.partition}")

    key_value_map = Map.get(state.data, state.partition, %{})
    {_, context} = Map.get(key_value_map, key, {nil, %{}})
    context = VClock.increment(Node.self(), context)
    IO.puts("#{inspect(context)}")
    # store key/value to my parition's key/value store
    {_, new_data} =
      state.data
      |> Map.get_and_update(state.partition, fn index_store ->
        {nil, Map.put(index_store, key, {value, context})}
      end)

    replicate_put(key, value, context, state.partition, state.replication, state.read)
    {:reply, :ok, %{state | data: new_data}}
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
  def handle_call({:get, key}, _from, state) do
    Logger.info("get #{key}")
    # TODO : Check R get values from other vnodes
    {value, context} =
      state.data
      |> Map.get(state.partition)
      |> Map.get(key, :key_not_found)

    Logger.info("Waiting for R replies")

    {value, state} =
      replicate_get(key, value, context, state.partition, state, state.replication, state.read)

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
