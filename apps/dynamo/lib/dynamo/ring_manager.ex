defmodule Ring.Manager do
  @moduledoc """
  Utility functions for Ring
  """

  use GenServer

  @type chash_node() :: term()
  @type index() :: <<_::160>>
  @type ring() :: Ring

  def start_link(opts) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  @impl true
  def init(:ok) do
    ring = Ring.new_ring()
    {:ok, ring}
  end

  @spec get_my_ring :: ring()
  def get_my_ring do
    GenServer.call(__MODULE__, :get_my_ring)
  end

  @spec set_my_ring(ring()) :: :ok
  def set_my_ring(new_ring) do
    GenServer.call(__MODULE__, {:set_my_ring, new_ring})
  end

  @doc """
  Return preference list of size n_val
  """
  @spec get_preference_list(index(), integer()) :: [chash_node()]
  def get_preference_list(index, n_val) do
    GenServer.call(__MODULE__, {:get_preference_list, index, n_val})
  end

  @spec ring_transform(function(), [term()]) :: none()
  def ring_transform(fun, args) do
    GenServer.call(__MODULE__, {:ring_transform, fun, args})
  end

  @impl true
  def handle_call(:get_my_ring, _from, ring) do
    {:reply, {:ok, ring}, ring}
  end

  @impl true
  def handle_call({:set_my_ring, new_ring}, _from, _prev_ring) do
    {:reply, new_ring, new_ring}
  end

  @impl true
  def handle_call({:get_preference_list, index, n_val}, _from, ring) do
    successors = CHash.successors(index, n_val, ring.chring)
    {:reply, successors, ring}
  end

  @impl true
  def handle_call({:ring_transform, fun, args}, _from, ring) do
    case fun.(ring, args) do
    # case Ring.Gossip.reconcile(ring, args) do
      {:reconciled_ring, new_ring} ->
        Ring.Gossip.recursive_gossip(new_ring)
        {:reply, new_ring, new_ring}
      :ignore ->
        # nothing has changed
        {:reply, :not_changed, ring}
      _ ->
        {:reply, :not_changed, ring}
    end
  end
end