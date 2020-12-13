defmodule Vnode.Replication do
  @moduledoc """
  This module stores the replication logics for put operation and
  read repair logic for get operation.
  """
  require Logger

  @type node_name() :: atom()
  @type vclock() :: map()

  @doc """
  Replicate operations to following vnodes
  """
  def replicate_put(key, value, context, state) do
    my_index = state.partition
    pref_list = Ring.Manager.get_self_exclusive_pref_list(my_index, state.replication - 1)
    num_write = min(length(pref_list) + 1, state.write)
    nonce = :erlang.phash2({key, value, context})

    Logger.info("Waiting for W replies")
    # spawn an asynchronous task for receiving the ack from replicating vnodes
    task =
      Dynamo.TaskSupervisor
      |> Task.Supervisor.async(fn ->
        wait_write_response(1, num_write, nonce)
      end)

    # send asynchronous replication task to other vnodes
    for {index, node} <- pref_list do
      GenServer.cast(
        {Vnode.Master, node},
        {:command, index,
          {:put_repl, my_index, key, value, context, task.pid, nonce, false}}
      )
    end

    :ok = Task.await(task, :infinity)
  end

  # this is for spawning async task for getting acks from other vnodes
  defp wait_write_response(current_write, num_write, _nonce) when current_write == num_write do
    :ok
  end

  defp wait_write_response(current_write, num_write, nonce) do
    # Logger.info("W responses: #{current_write}/#{num_write}")
    receive do
      {:ok, ^nonce} ->
        wait_write_response(current_write + 1, num_write, nonce)
      other ->
        wait_write_response(current_write, num_write, nonce)
    end
  end

  @read_repair_timeout 10

  def get_reponses(key, state) do
    my_node = Node.self()
    pref_list =
      CHash.hash_of(key)
      |> Ring.Manager.get_preference_list(state.replication)
    [{partition_index, _} | _] = pref_list

    num_read = min(length(pref_list), state.read)
    nonce = :erlang.phash2({key, :os.timestamp})

    Logger.info("Waiting for R replies")

    pid = self()
    # spawn a asynchronous task for receiving the ack from replicating vnodes
    {:ok, task_pid} =
      Dynamo.TaskSupervisor
      |> Task.Supervisor.start_child(fn ->
        responses = wait_read_response(0, num_read, nonce, [])
        {context, value, stale_nodes} = reconcile_values(responses)
        send(pid, {:ok, {context, value, stale_nodes}})

        if stale_nodes != :concurrent do
          Process.send_after(self(), :timeout, @read_repair_timeout)
          read_repair(num_read, state.replication, nonce, partition_index, context, key, value)
        end
      end)

    # send asynchronous replication task to other vnodes
    for {index, node} <- pref_list do
      if node == my_node do
          # This is to avoid deadlock from calling another sync command to the Vnode.Master
          # This can be mediated by coordinating get() from the client
          vpid = Vnode.Master.get_vnode_pid(index)
          GenServer.cast(vpid, {:get_repl, task_pid, partition_index, key, nonce, true})
      else
          GenServer.cast(
            {Vnode.Master, node},
            {:command, index,
              {:get_repl, task_pid, partition_index, key, nonce, true}}
          )
        end
    end

    receive do
      {:ok, {context, value, :concurrent}} ->
        {context, value, :concurrent}

      {:ok, {context, value, stale_nodes}} ->
        for node <- stale_nodes do
          GenServer.cast(node, {:put_repl, partition_index, key, value, context, nil, nil, true})
        end
        value
    end
  end

  defp read_repair(current_read, num_read, nonce, partition_index, latest_vclock, key, latest_value) do
    # Logger.info "Read repair #{current_read}, #{num_read}"
    if current_read < num_read do
      receive do
        {:ok, ^nonce, {context, {value, sender}}} ->
          if Vclock.compare_vclocks(context, latest_vclock) == :before do
            GenServer.cast(sender, {:put_repl, partition_index, key, latest_value, latest_vclock, nil, nil, true})
          end
          read_repair(current_read + 1, num_read, nonce, partition_index, latest_vclock, key, latest_value)
        :timeout ->
          # Logger.info "Read repair timee out."
          :timeout
        other ->
          Logger.info inspect other
          read_repair(current_read, num_read, nonce, partition_index, latest_vclock, key, latest_value)
      end
    end
  end

  defp wait_read_response(current_read, num_read, _nonce, responses) when current_read == num_read do
    responses
  end

  defp wait_read_response(current_read, num_read, nonce, responses) do
    # Logger.info "#{current_read}/#{num_read}"
    receive do
      {:ok, ^nonce, data} ->
        wait_read_response(current_read + 1, num_read, nonce, [data | responses])
      _ ->
        wait_read_response(current_read, num_read, nonce, responses)
    end
  end

  # Return reconciled values.
  # 1. All equal values: Single clock, single value, not stale vnodes
  # 2. Causal ordering: Single latest clock, single latest value, a list of stale vnodes
  # 3. Concurrent values: Merged clock, a list of concurrent values, :concurrent -> will send new put request
  @spec reconcile_values([]) :: {vclock(), [term()], [node_name()] | :concurrent}
  def reconcile_values(returned_values) do
    # Merge all clocks
    {vclocks, _} = Enum.unzip(returned_values)

    latest_vclocks = Vclock.get_latest_vclocks(vclocks)

    {latest_data, stale_data} =
      returned_values
      |> Enum.split_with(
        fn {context, _} -> context in latest_vclocks end)

    {_, stale_data} = Enum.unzip(stale_data)
    {_, stale_vnodes} = Enum.unzip(stale_data)

    {_, latest_data} = Enum.unzip(latest_data)
    {latest_values, _} = Enum.unzip(latest_data)

    if length(latest_vclocks) == 1 do
        [latest_vclock] = latest_vclocks
        [latest_value | _] = latest_values
        # IO.puts "Latest Value: #{inspect latest_values}"
        {latest_vclock, latest_value, stale_vnodes}
    else
        merged_vclock = Vclock.merge_vclocks(latest_vclocks)
        latest_values = latest_values |> Enum.concat |> Enum.uniq
        # latest_values = Enum.uniq(latest_values)
        {merged_vclock, latest_values, :concurrent}
    end
  end

  # This is a test code for retreiving all the values in the replication vnodes.
  def get_all_reads(key, state) do
    my_node = Node.self()
    pref_list =
      CHash.hash_of(key)
      |> Ring.Manager.get_preference_list(state.replication)
    [{partition_index, _} | _] = pref_list

    num_read = min(length(pref_list), state.read)
    nonce = :erlang.phash2({key, :os.timestamp})

    pid = self()
    {:ok, task_pid} =
      Dynamo.TaskSupervisor
      |> Task.Supervisor.start_child(fn ->
        responses = wait_read_response(0, state.replication, nonce, [])
        {context, value, stale_nodes} = reconcile_values(responses)
        send(pid, {:ok, {context, value, stale_nodes}})
      end)

    # send asynchronous replication task to other vnodes
    for {index, node} <- pref_list do
      if node == my_node do
          # This is to avoid deadlock from calling another sync command to the Vnode.Master
          # This can be mediated by coordinating get() from the client
          storage = Vnode.Master.get_partition_storage(index)
          {value, context} = Agent.get(storage, &Map.get(&1, key, {[], %{}}))
          send(task_pid, {:ok, nonce, {context, {value, self()}}})
      else
          GenServer.cast(
            {Vnode.Master, node},
            {:command, index,
              {:get_repl, task_pid, partition_index, key, nonce, true}}
          )
        end
    end

    receive do
      {:ok, {context, value, stale_nodes}} ->
        Enum.empty?(stale_nodes)
    end
  end
end
