defmodule Vnode.Master do
  @moduledoc """
  All the command messages to Vnode will go through vnode master
  """

  use GenServer

  @type index_as_int() :: integer()

  def start_link(opts) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  @doc """
  This delivers the command to the appropriate vnode synchronously.
  """
  @spec sync_command(index_as_int(), term()) :: term()
  def sync_command({index, _node}, msg) do
    pid = Vnode.Manager.get_vnode_pid(index)
    GenServer.call(pid, msg)
  end

  @impl true
  def init(:ok) do
    Vnode.Manager.start_ring
    {:ok, []}
  end
end