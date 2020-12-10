defmodule Vnode.Replication do
  require Logger

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
      # We need to add vclock or nonce for checking
      {:ok, ^nonce} ->
        wait_write_response(current_write + 1, num_write, nonce)

      other ->
        Logger.info("Wrong Response #{inspect(other)}")
        wait_write_response(current_write, num_write, nonce)
    end
  end

  @read_repair_timeout 10

  def replicate_get(key, value, context, state) do
    my_index = state.partition
    pref_list = Ring.Manager.get_self_exclusive_pref_list(my_index, state.replication - 1)
    num_read = min(length(pref_list) + 1, state.read)
    nonce = :erlang.phash2({key, value, context})

    Logger.info("Waiting for R replies")

    pid = self()
    # spawn a asynchronous task for receiving the ack from replicating vnodes
    {:ok, task_pid} =
      Dynamo.TaskSupervisor
      |> Task.Supervisor.start_child(fn ->
        wait_read_response(key, value, my_index, context, state, 1, num_read, pid, nonce)
      end)

    # send asynchronous replication task to other vnodes
    for {index, node} <- pref_list do
      GenServer.cast(
        {Vnode.Master, node},
        {:command, index,
          {:get_repl, task_pid, my_index, key, nonce, true}}
      )
    end

    receive do
      {:ok, value} ->
        # This is timer to stop read repair process.
        Dynamo.TaskSupervisor
        |> Task.Supervisor.start_child(fn ->
          Process.sleep(@read_repair_timeout)
          send(task_pid, :timeout)
        end)

        value
    end
  end

  defp wait_read_response(key, value, index, context, state, current_read, num_read, parent, nonce) do
    # Logger.info("R responses: #{current_read}/#{num_read}")

    # When R reponses has been returned send a message to `replicate_get` process.
    # But keep on receiving messages for read repair
    if current_read == num_read do
      send(parent, {:ok, value})
    end

    if current_read < state.replication do
      receive do
        {:ok, ^nonce, ^key, {other_value, other_context}, sender} ->
          case VClock.compare_vclocks(context, other_context) do
            :after ->
              # If my vlock is descendent of other vclock, put my value to the sender
              GenServer.cast(sender, {:put_repl, index, key, value, context, nil, nil, true})
              wait_read_response(key, value, index, context, state, current_read + 1, num_read, parent, nonce)

            :before ->
              # If my vlock is ancestor of other vclock, put other value to me
              # state = put_in(state, [:data, index, key], {other_value, other_context})
              storage = Vnode.Master.get_partition_storage(index)
              Agent.update(storage, &Map.put(&1, key, {other_value, other_context}))
              ActiveAntiEntropy.insert(key, other_value, index)
              wait_read_response(key, other_value, index, other_context, state, current_read + 1, num_read, parent, nonce)

            :equal when value == other_value ->
              wait_read_response(key, value, index, context, state, current_read + 1, num_read, parent, nonce)

            _ ->
              # If it's vclock is concurrent, merge two vclocks and increment my node
              # Also, add all the values to the node and do a read repair to the sender
              new_context =
                context
                |> VClock.merge_vclocks(other_context)
                |> VClock.increment(Node.self())

              new_value =
                ([value] ++ [other_value])
                |> List.flatten()
                |> Enum.uniq()
                |> Enum.sort()
                |> List.delete(nil)

              new_value =
                if length(new_value) == 1 do
                  Enum.take(new_value, 1)
                else
                  new_value
                end

              storage = Vnode.Master.get_partition_storage(index)
              Agent.update(storage, &Map.put(&1, key, {new_value, new_context}))
              GenServer.cast(sender, {:put_repl, index, key, new_value, new_context, nil, nil, true})

              wait_read_response(key, new_value, index, new_context, state, current_read + 1, num_read, parent, nonce)
          end
      :timeout ->
        # Logger.info("Timed out while waiting for R replies")
        :timeout
      end
    end
  end

  # This is a test code for retreiving all the values in the replication vnodes.
  def get_all_read(key, value, context, state) do
    my_index = state.partition
    pref_list = Ring.Manager.get_preference_list(my_index, state.replication - 1)
    current_read = 1
    nonce = :erlang.phash2({key, value, context})
    # spawn a asynchronous task for receiving the ack from replicating vnodes
    task =
      Dynamo.TaskSupervisor
      |> Task.Supervisor.async(fn ->
        wait_all_read_response(
          key,
          value,
          context,
          state,
          current_read,
          nonce,
          MapSet.new([value])
        )
      end)

    # send asynchronous replication task to other vnodes
    for {index, node} <- pref_list do
      GenServer.cast(
        {Vnode.Master, node},
        {:command, index,
          {:get_repl, task.pid, my_index, key, nonce, false}}
      )
    end

    Task.await(task)
  end

  defp wait_all_read_response(key, value, context, state, current_read, nonce, acc) do
    if current_read < state.replication do
      receive do
        {:ok, ^nonce, ^key, {^value, ^context}, _} ->
          wait_all_read_response(key, value, context, state, current_read + 1, nonce, acc)
        {:ok, ^nonce, ^key, {other_value, _}, _} ->
          wait_all_read_response(key, value, context, state, current_read + 1, nonce, MapSet.put(acc, other_value))
      end
    else
      acc = MapSet.to_list(acc)
      if length(acc) == 1 do
        Enum.at(acc, 0)
      else
        acc
      end
    end
  end
end
