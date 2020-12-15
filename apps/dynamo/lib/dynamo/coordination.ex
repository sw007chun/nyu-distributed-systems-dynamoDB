defmodule Coordination do
  require Logger

  @type node_name() :: atom()
  @type vclock() :: map()

  def wait_write_response(current_write, num_write) when current_write == num_write do
    Logger.info("W responses: #{current_write}/#{num_write}")
    :ok
  end

  def wait_write_response(current_write, num_write) do
    Logger.info("W responses: #{current_write}/#{num_write}")
    receive do
      :ok ->
        wait_write_response(current_write + 1, num_write)
      other ->
        Logger.info inspect other
        wait_write_response(current_write, num_write)
    end
  end

  def wait_read_response(current_read, num_read, responses) when current_read == num_read do
    Logger.info "R reponses: #{current_read}/#{num_read}"
    responses
  end

  def wait_read_response(current_read, num_read, responses) do
    Logger.info "R reponses: #{current_read}/#{num_read}"
    receive do
      {:ok, data} ->
        wait_read_response(current_read + 1, num_read, [data | responses])
      _ ->
        wait_read_response(current_read, num_read, responses)
    end
  end

  def now() do
    {mega, seconds, ms} = :os.timestamp()
    (mega * 1_000_000 + seconds) * 1000 + :erlang.round(ms / 1000)
  end

  @read_repair_timeout 10

  def get_reponses(key, node, state, supervisor) do
    Logger.info("Waiting for R replies")

    pid = self()
    # spawn a asynchronous task for receiving the ack from replicating vnodes
    {:ok, task_pid} =
      supervisor
      |> Task.Supervisor.start_child(fn ->
        responses = wait_read_response(0, state.read, [])
        {context, value, stale_nodes} = reconcile_values(responses)
        send(pid, {:ok, {context, value, stale_nodes}})

        if state.read_repair and stale_nodes != :concurrent do
          Process.send_after(self(), :timeout, @read_repair_timeout)
          read_repair(state.read, state.replication, context, key, value)
        end
      end)

    GenServer.cast({DynamoServer, node}, {:get, key, task_pid})

    receive do
      {:ok, {context, value, :concurrent}} ->
        {context, value, :concurrent}

      {:ok, {context, value, stale_vnodes}} ->
        if state.read_repair do
          for vnode <- stale_vnodes do
            GenServer.cast(vnode, {:put_repl, key, value, context, nil, true})
          end
        end
        value
    end
  end

  defp read_repair(current_read, num_read, latest_vclock, key, latest_value) do
    # Logger.info "Read repair #{current_read}, #{num_read}"
    if current_read < num_read do
      receive do
        {:ok, {context, {_, sender}}} ->
          if Vclock.compare_vclocks(context, latest_vclock) == :before do
            GenServer.cast(sender, {:put_repl, key, latest_value, latest_vclock, nil, true})
          end
          read_repair(current_read + 1, num_read, latest_vclock, key, latest_value)
        :timeout ->
          # Logger.info "Read repair timee out."
          :timeout
        other ->
          Logger.info inspect other
          read_repair(current_read, num_read, latest_vclock, key, latest_value)
      end
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
        {latest_vclock, latest_value, stale_vnodes}
    else
        merged_vclock = Vclock.merge_vclocks(latest_vclocks)
        latest_values = latest_values |> Enum.concat |> Enum.uniq
        {merged_vclock, latest_values, :concurrent}
    end
  end
end
