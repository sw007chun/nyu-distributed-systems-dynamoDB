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

  @impl true
  def init(:ok) do
    Vnode.Manager.start_ring()
    {:ok, []}
  end

  @impl true
  def handle_cast({:command, index, msg}, state) do
    Logger.info("#{Node.self()} received command #{inspect(msg)}")
    pid = Vnode.Manager.get_vnode_pid(index)
    GenServer.cast(pid, msg)
    {:noreply, state}
  end

  # Find pid of vnode responsible for hat index and send command.
  @impl true
  def handle_call({:sync_command, index, msg}, _from, state) do
    Logger.info("#{Node.self()} received sync command #{inspect(msg)}")
    pid = Vnode.Manager.get_vnode_pid(index)
    result = GenServer.call(pid, msg, :infinity)
    {:reply, result, state}
  end
end
