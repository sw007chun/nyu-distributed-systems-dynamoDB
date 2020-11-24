defmodule Ring.Manager do
  @moduledoc """
  Utility functions for Ring
  """

  use GenServer

  @type chash_node() :: term()

  def start_link(opts) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  @doc """
  Return preference list of size n_val
  """
  @spec get_preference_list(integer(), integer()) :: [chash_node()]
  def get_preference_list(index, n_val) do
    GenServer.call(__MODULE__, {:get_preference_list, index, n_val})
  end

  @doc """
  Return current ring status
  """
  def get_ring do
    GenServer.call(__MODULE__, :get_ring)
  end

  @impl true
  def init(:ok) do
    ring = Ring.new_ring()
    {:ok, ring}
  end

  @impl true
  def handle_call({:get_preference_list, index, n_val}, _from, ring) do
    successors = CHash.successors(index, n_val, ring.chring)
    {:reply, successors, ring}
  end

  @impl true
  def handle_call(:get_ring, _from, ring) do
    {:reply, ring, ring}
  end

end