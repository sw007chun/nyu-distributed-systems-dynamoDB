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

  # Vnode keeps state partition, the index of partition it's in charge
  # and data, a key/value store
  @impl true
  def init(partition) do
    {:ok, %{:partition => partition, :data => %{}}}
  end

  @impl true
  def handle_call(:ping, _from, state) do
    {:reply, {:pong, state.partition}, state}
  end

  @impl true
  def handle_call({:put, key, value}, _from, state) do
    IO.puts "put #{key}: #{value}"
    new_data = Map.put(state.data, key, value)
    {:reply, :ok, %{state | data: new_data}}
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    IO.puts "get #{key}"
    {:reply, Map.get(state.data, key, :key_not_found), state}
  end

  @impl true
  def handle_call({:delete, key}, _from, state) do
    IO.puts "delete #{key}"
    new_data = Map.delete(state.data, key)
    {:reply, Map.get(state.data, key, :key_not_found), %{state | data: new_data}}
  end
end