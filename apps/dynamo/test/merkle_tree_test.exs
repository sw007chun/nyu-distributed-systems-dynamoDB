defmodule MerkleTreeTest do
  use ExUnit.Case, async: true

  test "simple" do
    a0 = MerkleTree.new(0)
    b0 = MerkleTree.new(1)

    a1 = MerkleTree.insert(a0, :hello, "world")
    a2 = MerkleTree.insert(a1, :hello, "world")
    a3 = MerkleTree.insert(a2, :distributed, "systems")

    b1 = MerkleTree.insert(b0, :amazon, "s3")
    b2 = MerkleTree.insert(b1, :amazon, "dynamo")
    b3 = MerkleTree.insert(b2, :hello, "dynamo")
    b4 = MerkleTree.insert(b3, "will be", "deleted")

    a4 = MerkleTree.update_tree(a3)
    b5 = MerkleTree.update_tree(b4)

    b6 = MerkleTree.delete(b5, "will be")
    b7 = MerkleTree.update_tree(b6)

    keys =
      [:hello, :distributed, :amazon]
      |> Enum.map(&:erlang.phash2/1)
      |> Enum.map(&(rem(&1, a0.segments)))
      |> Enum.sort

    diff = MerkleTree.compare_trees({a4.tree, b7.tree})
    assert keys == diff

    a_segments = MerkleTree.get_segments(a4.path, diff)
    b_segments = MerkleTree.get_segments(b7.path, diff)

    assert [different: :hello, missing: :amazon, other_missing: :distributed] == MerkleTree.compare_segments(a_segments, b_segments)

    MerkleTree.destroy(a0)
    MerkleTree.destroy(b0)
  end
end
