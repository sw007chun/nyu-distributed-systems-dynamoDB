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
  This delivers the command to the appropriate node synchronously.
  """
  @spec sync_command(index_as_int(), term()) :: term()
  def sync_command({index, node}, msg) do
    GenServer.call({__MODULE__, node}, {:sync_command, Node.self(), index, msg})
  end

  @impl true
  def init(:ok) do
    Vnode.Manager.start_ring
    {:ok, []}
  end

  # Find pid of vnode responsible for hat index and send command.
  @impl true
  def handle_call({:sync_command, sender, index, msg}, _from, state) do
    IO.puts "Received command #{inspect(msg)} from #{sender}"
    pid = Vnode.Manager.get_vnode_pid(index)
    result = GenServer.call(pid, msg)
    {:reply, result, state}
  end
end