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
      GenServer.cast(
        {Vnode.Master, node},
        {:command, index,
          {:put_repl, task.pid, my_index, key, value, context, nonce}}
      )
    end

    :ok = Task.await(task, :infinity)
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

  @read_repair_timeout 50

  def replicate_get(key, value, context, state) do
    my_index = state.partition
    pref_list = Ring.Manager.get_preference_list(my_index, state.replication - 1)
    nonce = :erlang.phash2({key, value, context})

    Logger.info("Waiting for R replies")

    pid = self()
    # spawn a asynchronous task for receiving the ack from replicating vnodes
    {:ok, task_pid} =
      Dynamo.TaskSupervisor
      |> Task.Supervisor.start_child(fn ->
        wait_read_response(key, value, my_index, context, state, 1, pid, nonce)
      end)

    # send asynchronous replication task to other vnodes
    for {index, node} <- pref_list do
      GenServer.cast(
        {Vnode.Master, node},
        {:command, index,
          {:get_repl, task_pid, my_index, key, nonce, true}}
      )
    end

    Process.send_after(task_pid, :timeout, @read_repair_timeout)

    receive do
      {:ok, {value, state}} -> {value, state}
    end
  end

  defp wait_read_response(key, value, index, context, state, current_read, parent, nonce) do
    Logger.info("R responses: #{current_read}/#{state.read}")

    if current_read == state.read do
      send(parent, {:ok, {value, state}})
    end

    if current_read < state.replication do
      receive do
        {:ok, ^nonce, ^key, {other_value, other_context}, other_index, sender} ->
          case VClock.compare_vclocks(context, other_context) do
            :after ->
              GenServer.cast(
                {Vnode.Master, sender},
                {:command, other_index,
                  {:read_repair, key, value, index, context}}
              )

              wait_read_response(key, value, index, context, state, current_read + 1, parent, nonce)

            :before ->
              state = put_in(state, [:data, index, key], {other_value, other_context})
              wait_read_response(key, other_value, index, other_context, state, current_read + 1, parent, nonce)

            :equal when value == other_value ->
              wait_read_response(key, value, index, context, state, current_read + 1, parent, nonce)

            _ ->
              Logger.info("Vclock Concurrent")
              Logger.info("#{value}, #{other_value}")
              Logger.info("#{inspect(context)},...,#{inspect(other_context)}")
              # Merging concurrent vclocks into one
              # Incrementing the vclock and adding all concurrent values to a list
              # Sending the update replica command back to the node
              new_context =
                context
                |> VClock.merge_vclocks(other_context)
                |> VClock.increment(Node.self())

              new_value =
                ([value] ++ [other_value])
                |> List.flatten()
                |> Enum.uniq()

              new_value =
                if length(new_value) == 1 do
                  Enum.take(new_value, 1)
                else
                  new_value
                end

              state = put_in(state, [:data, index, key], {new_value, new_context})

              GenServer.cast(
                {Vnode.Master, sender},
                {:command, other_index,
                  {:read_repair, key, new_value, index, new_context}}
              )

              wait_read_response(key, new_value, index, new_context, state, current_read + 1, parent, nonce)
          end
      :timeout ->
        Logger.info("Timed out while waiting for R replies")
        :ok
      end
    else
      :ok
    end
  end

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
          my_index,
          context,
          state,
          current_read,
          state.replication
        )
      end)

    # send asynchronous replication task to other vnodes
    for {index, node} <- pref_list do
      {Dynamo.TaskSupervisor, node}
      |> Task.Supervisor.start_child(Vnode.Master, :command, [
        {index, node},
        {:get_repl, task.pid, my_index, key, nonce, false}
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
