defmodule Vnode.Replication do
  require Logger


  @doc """
  Replicate operations to following vnodes
  """
  def replicate_put(key, value, context, my_index, num_replication, num_write) do
    pref_list = Ring.Manager.get_preference_list(my_index, num_replication - 1)
    nonce = :erlang.phash2({key, value, context})

    # spawn an asynchronous task for receiving the ack from replicating vnodes
    task =
      Dynamo.TaskSupervisor
      |> Task.Supervisor.async(fn ->
        wait_write_response(1, num_write, nonce)
      end)

    # send asynchronous replication task to other vnodes
    for {index, node} <- pref_list do
        {Dynamo.TaskSupervisor, node}
        |> Task.Supervisor.start_child(Vnode.Master, :command, [
          {index, node},
          {:put_repl, task.pid, my_index, key, value, context, nonce}
        ])
    end

    :ok = Task.await(task)
  end

  # this is for spawning async task for getting acks from other vnodes
  defp wait_write_response(current_write, num_write, _nonce) when current_write == num_write do
    :ok
  end

  defp wait_write_response(current_write, num_write, nonce) do
    receive do
      # We need to add vclock or nonce for checking
      {:ok, ^nonce} ->
        wait_write_response(current_write + 1, num_write, nonce)

      other ->
        Logger.info("#{inspect(other)}")
        wait_write_response(current_write, num_write, nonce)
    end
  end

  def replicate_get(key, value, context, my_index, state, num_replication, num_read) do
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
        {Dynamo.TaskSupervisor, node}
        |> Task.Supervisor.start_child(Vnode.Master, :command, [
          {index, node},
          {:get_repl, task.pid, my_index, key, true}
        ])
    end

    Task.await(task)
  end

  defp wait_read_response(key, value, index, context, state, current_read, num_read) do
    Logger.info "current: #{current_read}/#{num_read}"
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
                |> Enum.uniq

              new_value =
                if length(new_value) do
                  Enum.take(new_value, 1)
                else
                  new_value
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
        1_000 ->
          Logger.info("Timed out while waiting for R replies")
          {value, state}
      end
    else
      {value, state}
    end
  end

  def get_all_read(key, value, context, my_index, state, num_replication, num_read) do
    pref_list = Ring.Manager.get_preference_list(my_index, num_replication - 1)
    current_read = 1

    # spawn a asynchronous task for receiving the ack from replicating vnodes
    task =
      Dynamo.TaskSupervisor
      |> Task.Supervisor.async(fn ->
        wait_all_read_response(key, value, my_index, context, state, current_read, num_replication)
      end)

    # send asynchronous replication task to other vnodes
    for {index, node} <- pref_list do
        {Dynamo.TaskSupervisor, node}
        |> Task.Supervisor.start_child(Vnode.Master, :command, [
          {index, node},
          {:get_repl, task.pid, my_index, key, false}
        ])
    end

    Task.await(task)
  end

  defp wait_all_read_response(key, value, my_index, context, state, current_read, num_read) do
    if current_read < num_read do
      receive do
        {:ok, ^key, {^value, ^context}, other_index, sender} ->
          wait_all_read_response(key, value, my_index, context, state, current_read + 1, num_read)
        _ ->
          false
      end
    else
      true
    end
  end

end
