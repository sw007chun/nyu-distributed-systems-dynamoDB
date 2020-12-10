defmodule Ring.Claimant do
  # use GenServer

  @type ring() :: Ring
  @type node_name() :: atom()
  @type index_as_int() :: non_neg_integer()
  @type node_entry() :: {index_as_int(), node_name()}

  @spec ring_changed(node_name(), ring()) :: ring()
  def ring_changed(node, ring) do
    {_changed, new_ring} = do_claimant(node, ring)
    new_ring
  end

  def do_claimant(node, ring) do
    joining_nodes = get_joining_nodes(ring)
    {changed_join, ring_2} = handle_joining(node, ring, joining_nodes)
    leaving_nodes = get_leaving_nodes(ring_2)
    {changed_leave, ring_3} = handle_remove(node, ring_2, leaving_nodes)

    {changed_join or changed_leave, rebalance(ring_3)}

    # case joining_nodes == [] do
    #   true ->
    #     {true, rebalance(ring_2)}
    #   false ->
    #     {changed, ring_2}
    # end
  end

  @spec get_joining_nodes(ring()) :: [node_name()]
  def get_joining_nodes(ring) do
    Ring.members(ring, [:joining])
  end

  @spec get_leaving_nodes(ring()) :: [node_name()]
  def get_leaving_nodes(ring) do
    Ring.members(ring, [:leaving])
  end

  # Joining nodes to valid nodes
  def handle_joining(node, ring, joining_nodes) do
    # case Ring.claimant(ring) do
    #   ^node ->
    changed = Enum.empty?(joining_nodes)

    new_ring =
      joining_nodes
      |> Enum.reduce(
        ring,
        fn join_node, ring0 ->
          Ring.set_member(node, ring0, join_node, :valid)
        end
      )

    {changed, new_ring}
    #   _ ->
    #     {false, ring}
    # end
  end

  def handle_remove(node, ring, leaving_nodes) do
    changed = Enum.empty?(leaving_nodes)

    new_ring =
      leaving_nodes
      |> Enum.reduce(
        ring,
        fn leave_node, ring0 ->
          Ring.set_member(node, ring0, leave_node, :invalid)
        end
      )

    {changed, new_ring}
  end

  def rebalance(ring) do
    Ring.claiming_members(ring)
    |> Enum.reduce(
      ring,
      fn node, ring ->
        claim(ring, node)
      end
    )
  end

  def claim(ring, node) do
    case needs_claim(ring, node) do
      true ->
        choose_claim(ring, node)

      false ->
        ring
    end
  end

  @doc """
  Checks if a node has lower than average vnodes.
  """
  def needs_claim(ring, node) do
    active = Ring.claiming_members(ring)
    owners = Ring.all_index_owners(ring)
    counter = get_counts(active, owners)

    num_active_nodes = length(active)
    ring_size = Ring.num_partitions(ring)
    average_count = div(ring_size, num_active_nodes)

    Map.get(counter, node) < average_count
  end

  @doc """
  Reassign nodes to indices
  """
  @spec choose_claim(ring(), node_name()) :: ring()
  def choose_claim(ring, node) do
    active_nodes =
      [node | Ring.claiming_members(ring)]
      |> Enum.uniq()
      |> Enum.sort()

    cyclic_distribution(ring, active_nodes)
    |> Enum.reduce(
      ring,
      fn {index, new_owner}, ring0 ->
        Ring.transfer_node(index, new_owner, ring0)
      end
    )
  end

  @doc """
  distribute index in cyclic way to make nodes most disperse
  """
  @spec cyclic_distribution(ring(), [node_name()]) :: [{index_as_int(), node_name()}]
  def cyclic_distribution(ring, nodes) do
    partition_size = Ring.num_partitions(ring)
    cyclic_nodes = Enum.take(Stream.cycle(nodes), partition_size)

    indices = Ring.all_indices(ring) |> Enum.sort()
    Enum.zip(indices, cyclic_nodes)
  end

  # Count the number of partitions owned by each active node.
  @spec get_counts([node_name()], [node_entry()]) :: %{node_name() => non_neg_integer()}
  def get_counts(nodes, partition_owners) do
    counter = Map.new(nodes, fn node -> {node, 0} end)

    partition_owners
    |> Enum.reduce(
      counter,
      fn {_index, owner}, counter0 ->
        case Enum.member?(nodes, owner) do
          true ->
            Map.update(counter0, owner, 1, &(&1 + 1))

          false ->
            counter0
        end
      end
    )
  end
end
