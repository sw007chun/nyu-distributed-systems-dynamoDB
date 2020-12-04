defmodule CHashTest do
  use ExUnit.Case, async: true

  test "simple size test" do
    assert length(CHash.new_ring(8, :the_node).node_entries) == 8
    assert CHash.size(CHash.new_ring(5, :the_node)) == 6
  end

  test "update test" do
    node = "old@host"
    new_node = "new@host"

    chash = CHash.new_ring(5, node)
    {:ok, {first_index, _}} = Enum.fetch(chash.node_entries, 0)
    {:ok, {third_index, _}} = Enum.fetch(chash.node_entries, 2)

    first_update = CHash.update_owner(first_index, new_node, chash)
    third_update = CHash.update_owner(third_index, new_node, chash)
    %CHash{num_partitions: 5, node_entries: [{_, ^new_node}, {_, ^node}, {_, ^node}, {_, ^node}, {_, ^node}, {_, ^node}]} =
        first_update
    %CHash{num_partitions: 5, node_entries: [{_, ^node}, {_, ^node}, {_, ^new_node}, {_, ^node}, {_, ^node}, {_, ^node}]} =
        third_update

    assert CHash.lookup(first_index, first_update) == new_node
    assert CHash.lookup(third_index, third_update) == new_node

    assert CHash.members(first_update) == [new_node, node]
    assert CHash.members(third_update) == [new_node, node]
  end

  test "successor test" do
    node = "old@host"
    new_node = "new@host"

    partition_size = 8
    chash = CHash.new_ring(partition_size, node)
    {:ok, {index, _}} = Enum.fetch(chash.node_entries, 3)
    updated_ring = CHash.update_owner(index, new_node, chash)
    bin_index = <<(index - 10)::160>>
    successors = CHash.successors(bin_index, partition_size, updated_ring)

    assert length(successors) == partition_size
    assert {:ok, {_, ^new_node}} = Enum.fetch(successors, 0)
    assert CHash.predecessors(<<0::160>>, partition_size, chash) == Enum.reverse(CHash.successors(<<0::160>>, partition_size, chash))
    assert CHash.predecessors(CHash.key_of(4), partition_size, updated_ring) == Enum.reverse(CHash.successors(CHash.key_of(4), partition_size, updated_ring))
  end
end