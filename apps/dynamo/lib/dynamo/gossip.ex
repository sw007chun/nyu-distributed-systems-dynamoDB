defmodule Ring.Gossip do
  @moduledoc """
  Gossip protocol
  """
  use GenServer
  @type ring() :: Ring

  def start_link(opts) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  # Send a ring to other remote node
  def send_ring(ring, other_node) do
    GenServer.cast({__MODULE__, other_node}, {:send_ring_to, ring})
  end

  @doc """
  Reconciles two rings. Normally called when the node received
  a new ring from gossip
  """
  @spec reconcile(ring(), ring()) :: none()
  def reconcile(my_ring, other_ring) do
    my_node = Node.self()
    other_node = Ring.owner_node(other_ring)
    members = Ring.reconcile_members(my_ring, other_ring)

    pre_status = Ring.member_status(members, other_node)
    ignore_gossip = (pre_status == :invalid) or (pre_status == :down)

    {changed, new_ring} =
      case ignore_gossip do
        true ->
          {false, my_ring}
        false ->
          Ring.reconcile(my_ring, other_ring)
      end

    # IO.puts changed

    case changed do
      :new_ring ->
        # Skipped Ring.ring_ready
        ring_2 = Ring.Claimant.ring_changed(my_node, new_ring)
        {:reconciled_ring, ring_2}
      _ ->
        :ignore
    end
  end

  @doc """
  Send ring information to two other nodes.
  Nodes that received will gossip recursively until
  it has received the same ring from gossip.
  """
  @spec recursive_gossip(ring()) :: :ok
  def recursive_gossip(ring) do
    my_node = Node.self()
    active_nodes = Ring.active_members(ring)
    case Enum.member?(active_nodes, my_node) do
      true ->
        recursive_gossip(ring, my_node)
      false ->
        random_recursive_gossip(ring)
    end
  end

  def recursive_gossip(ring, node) do
    active_nodes = Ring.active_members(ring)
    children = get_children(active_nodes, node)
    for other_node <- children do
      send_ring(ring, other_node)
    end
    :ok
  end

  def random_recursive_gossip(ring) do
    random_node = Ring.active_members(ring) |> Enum.random
    recursive_gossip(ring, random_node)
  end

  # Consider nodes as a binary tree made with a list.
  # Return the child of parent_node at index 2i, and 2i + 1
  # The leaf nodes will have child as a wrap arounded value
  def get_children(nodes, parent_node) do
    expanded =
      nodes
      |> List.duplicate(3)
      |> List.flatten

    index = Enum.find_index(expanded, fn x -> x == parent_node end)
    Enum.slice(expanded, 2 * index + 1, 2)
  end

  @impl true
  def init(:ok) do
    state = []
    {:ok, state}
  end

  @impl true
  def handle_cast({:send_ring_to, other_ring}, state) do
    Ring.Manager.ring_transform(&reconcile/2, other_ring)
    {:noreply, state}
  end
end
