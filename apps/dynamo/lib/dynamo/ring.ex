defmodule Ring do
  @moduledoc """
  Actual implementation of ring
  """


  defstruct(
    node_name: nil,
    vclock: nil,
    chring: nil
  )

  @type chash_node() :: term()
  @type index_as_int() :: integer()
  @type node_entry() :: {index_as_int(), chash_node()}

  @doc """
  Return all the node entries
  """
  @spec all_owners(%Ring{}) :: [node_entry()]
  def all_owners(ring) do
    CHash.nodes(ring.chring)
  end

  @doc """
  Return all the indices of partitions in the ring
  """
  @spec my_indices(%Ring{}) :: [index_as_int()]
  def my_indices(ring) do
    for {index, owner_node} <- all_owners(ring),
        owner_node === Node.self() do
      index
    end
  end

  @doc """
  Make new ring with current node as the seed node
  """
  def new_ring do
    new_ring(Node.self())
  end

  @doc """
  Make new ring with current node as the seed node.
  If ring size is unspecified, import from config.exs file
  """
  def new_ring(node_name) do
    new_ring(Application.get_env(:dynamo, :ring_size), node_name)
  end

  def new_ring(ring_size, node_name) do
    vclock = VClock.increment(node_name, VClock.new_clock)
    # TODO: add gossip
    %Ring{node_name: node_name, vclock: vclock, chring: CHash.new_ring(ring_size, node_name)}
  end
end