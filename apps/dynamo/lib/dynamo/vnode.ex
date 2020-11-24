defmodule Vnode do
  @moduledoc """
  Actual methods for vnodes
  """

  use GenServer

  @doc """
  Start Vnode for partition starting at index.
  """
  @spec start_vnode(integer()) :: pid()
  def start_vnode(index) do
    Vnode.Manager.get_vnode_pid(index)
  end

  def start_link(index) do
    GenServer.start_link(__MODULE__, index)
  end

  @impl true
  def init(partition) do
    {:ok, %{:partition => partition}}
  end

  @impl true
  def handle_call(:ping, _from, state) do
    IO.puts "Received ping command #{state.partition}"
    {:reply, {:pong, state.partition}, state}
  end
end