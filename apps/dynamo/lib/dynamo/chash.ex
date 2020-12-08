defmodule CHash do
  @moduledoc """
  Consistent hashing module
  Elixir version of
  https://github.com/basho/riak_core/blob/develop-3.0/src/chash.erl
  """

  # Max valud of a hash key
  @ringtop trunc(:math.pow(2, 160) - 1)

  @typedoc """
  Unique identifier fore the owner of a given partition.
  """
  @type chash_node() :: term()
  @typedoc """
  Index to an object location in the ring as 160-bitstring
  """
  @type index() :: <<_::160>>
  @type index_as_int() :: integer()
  @type node_entry() :: {index_as_int(), chash_node()}

  defstruct(
    num_partitions: 0,
    node_entries: []
  )

  @doc """
  Return a new ring object.
  """
  @spec new_ring(pos_integer(), chash_node()) :: %CHash{}
  def new_ring(num_partitions, seed_node) do
    inc = ring_increment(num_partitions)
    # there is a one more partition if num_partitions is odd. Not sure how it's managed for now.
    node_entries =
      for index_as_int <- :lists.seq(0, @ringtop - 1, inc) do
        {index_as_int, seed_node}
      end

    %CHash{
      num_partitions: num_partitions,
      node_entries: node_entries
    }
  end

  @doc """
  Return indices of the ring
  """
  @spec all_indices(%CHash{}) :: [index_as_int()]
  def all_indices(chash) do
    for {index, _owner} <- chash.node_entries, do: index
  end

  @doc """
  Return SHA-1 hash value of an object
  """
  @spec key_of(term()) :: index()
  def key_of(object_name) do
    # argument should be converted to binary
    :crypto.hash(:sha, :erlang.term_to_binary(object_name))
  end

  @doc """
  Find the node the owns the partition identified by the index
  """
  @spec lookup(index_as_int(), %CHash{}) :: chash_node()
  def lookup(index_as_int, chash) do
    {^index_as_int, node} = List.keyfind(chash.node_entries, index_as_int, 0)
    node
  end

  @doc """
  Return all the nodes that own any partitions in the ring.
  """
  @spec members(%CHash{}) :: [chash_node()]
  def members(chash) do
    Enum.uniq(for {_, node} <- chash.node_entries, do: node)
    |> Enum.sort()
  end

  @doc """
  Given hash key value of the object
  return the index of next partition node
  """
  @spec next_index(integer(), %CHash{}) :: index_as_int()
  def next_index(integer_key, chash) do
    num_partitions = chash.num_partitions
    inc = ring_increment(num_partitions)
    rem(div(integer_key, inc) + 1, num_partitions) * inc
  end

  @doc """
  Return the node_entries
  """
  @spec nodes(%CHash{}) :: [node_entry()]
  def nodes(chash) do
    chash.node_entries
  end

  @doc """
  Return index increment between two subsequent partitions
  """
  @spec ring_increment(pos_integer()) :: pos_integer()
  def ring_increment(num_partitions) do
    div(@ringtop, num_partitions)
  end

  @doc """
  Return the number of partitions in the ring.
  """
  @spec size(%CHash{}) :: pos_integer()
  def size(chash) do
    length(chash.node_entries)
  end

  @doc """
  Return previous n node entries.
  This is used for preference list
  """
  @spec predecessors(index(), non_neg_integer(), %CHash{}) :: [node_entry()]
  def predecessors(index, n_val, chash) do
    num = min(n_val, chash.num_partitions)
    <<index_as_int::160>> = index
    inc = ring_increment(chash.num_partitions)
    next_partition_index = div(index_as_int, inc) + 1
    {first_n, following} = Enum.split(chash.node_entries, next_partition_index)

    (following ++ first_n)
    |> Enum.reverse()
    |> Enum.take(num)
  end

  @doc """
  Return following n node entries.
  This is used for preference list
  """
  @spec successors(index(), non_neg_integer(), %CHash{}) :: [node_entry()]
  def successors(index, n_val, chash) do
    num = min(n_val, chash.num_partitions)
    <<index_as_int::160>> = index
    inc = ring_increment(chash.num_partitions)
    next_partition_index = div(index_as_int, inc) + 1
    {first_n, following} = Enum.split(chash.node_entries, next_partition_index)
    Enum.take(following ++ first_n, num)
  end

  @doc """
  Replace the owner of the partition at index
  """
  @spec update_owner(index_as_int(), chash_node(), %CHash{}) :: %CHash{}
  def update_owner(index_as_int, new_node, chash) do
    new_node_entries =
      List.keyreplace(
        chash.node_entries,
        index_as_int,
        0,
        {index_as_int, new_node}
      )

    %{chash | node_entries: new_node_entries}
  end
end
