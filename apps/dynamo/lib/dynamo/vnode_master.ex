defmodule Vnode.Master do
  @moduledoc """
  All the command messages to Vnode will go through vnode master
  """

  use GenServer
  require Logger

  @type index_as_int() :: integer()

  def start_link(opts) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  @spec command(index_as_int(), term()) :: term()
  def command({index, node}, msg) do
    GenServer.cast({__MODULE__, node}, {:command, index, msg})
  end

  @doc """
  This delivers the command to the appropriate node synchronously.
  """
  @spec sync_command(index_as_int(), term()) :: term()
  def sync_command({index, node}, msg) do
    GenServer.call({__MODULE__, node}, {:sync_command, index, msg}, :infinity)
  end

  @doc """
  Return key/value store of certain partition.
  """
  @spec get_my_data(non_neg_integer()) :: %{}
  def get_my_data(partition) do
    get_partition_storage(partition)
    |> Agent.get(& &1)
  end

  # Return pid of Agent storing {key, value} pair of partition
  @spec get_partition_storage(non_neg_integer()) :: pid()
  def get_partition_storage(partition) do
    case Registry.lookup(Registry.Vnode, {"Storage", partition}) do
      [{pid, _}] ->
        pid

      [] ->
        # TODO: check partition number with the ring
        name = {:via, Registry, {Registry.Vnode, {"Storage", partition}, :storage}}

        case Agent.start_link(fn -> %{} end, name: name) do
          {:ok, pid} ->
            pid

          {:error, {:already_started, pid}} ->
            Registry.register(Registry.Vnode, {"Storage", partition}, :storage)
            pid
        end
    end
  end

  @doc """
  Clear all data in the storage.
  """
  def clear_storage do
    storage_list = Registry.select(Registry.Vnode, [{{:_, :"$2", :"$3"}, [{:"==", :"$3", :storage}], [:"$2"]}])
    for storage <- storage_list do
      Agent.update(storage, fn _ -> %{} end)
    end
  end

  @doc """
  Reset vnode parameters.
  """
  @spec reset_vnode_param([{term(), term()}]) :: :ok
  def reset_vnode_param(param_list) do
    vnode_list = Registry.select(Registry.Vnode, [{{:_, :"$2", :"$3"}, [{:"==", :"$3", :vnode}], [:"$2"]}])
    for vnode <- vnode_list do
      GenServer.call(vnode, {:reset_param, param_list})
    end
    :ok
  end

  # Return pid of vnode in charge of partition
  @spec get_vnode_pid(non_neg_integer()) :: pid()
  def get_vnode_pid(partition) do
    case Registry.lookup(Registry.Vnode, partition) do
      [{pid, _}] ->
        pid

      _ ->
        {:ok, pid} = DynamicSupervisor.start_child(Vnode.Supervisor, {Vnode, partition})
        pid
    end
  end

  @impl true
  def init(:ok) do
    {:ok, ring} = Ring.Manager.get_my_ring()
    startable_vnodes = Ring.my_indices(ring)

    for index <- startable_vnodes do
      DynamicSupervisor.start_child(Vnode.Supervisor, {Vnode, index})
    end

    {:ok, []}
  end

  @impl true
  def handle_cast({:command, index, msg}, state) do
    pid = get_vnode_pid(index)
    GenServer.cast(pid, msg)
    {:noreply, state}
  end

  # Find pid of vnode responsible for hash index and send command.
  @impl true
  def handle_call({:sync_command, index, msg}, _from, state) do
    pid = get_vnode_pid(index)
    result = GenServer.call(pid, msg, :infinity)
    {:reply, result, state}
  end
end
