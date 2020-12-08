defmodule Vnode.Manager do
  @moduledoc """
  Utility function for vnodes
  """

  use GenServer

  @type index_as_int() :: integer()

  def start_link(opts) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  def stop do
    GenServer.cast(__MODULE__, :stop)
  end

  @doc """
  Return the PID of partition index
  """
  @spec get_vnode_pid(index_as_int()) :: pid()
  def get_vnode_pid(index) do
    GenServer.call(__MODULE__, {:get_vnode, index}, :infinity)
  end

  @doc """
  This is part of initialization process. It starts all the vnode.
  """
  def start_ring do
    {:ok, ring} = Ring.Manager.get_my_ring()
    startable_vnodes = Ring.my_indices(ring)

    for index <- startable_vnodes do
      Vnode.start_vnode(index)
    end
  end

  @impl true
  def init(:ok) do
    {:ok, %{}}
  end

  # Return the PID of parition index.
  # If the index is not in the index_table, start the vnode first
  # and update the index_table and return the pid
  @impl true
  def handle_call({:get_vnode, index}, _from, index_table) do
    {vnode_pid, index_table} =
      case Map.get(index_table, index, :none) do
        :none ->
          {:ok, new_pid} = DynamicSupervisor.start_child(Vnode.Supervisor, {Vnode, index})
          new_table = Map.put(index_table, index, new_pid)
          {new_pid, new_table}

        pid ->
          {pid, index_table}
      end

    {:reply, vnode_pid, index_table}
  end
end
