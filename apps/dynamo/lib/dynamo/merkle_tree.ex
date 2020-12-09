defmodule MerkleTree do
  @num_segments 4
  @width 2
  @num_level 2
  @path "key_store"

  defstruct(
    index: 0,
    path: nil,
    segments: @num_segments,
    width: @width,
    levels: @num_level,
    tree: %{},
    write_buffer: []
  )

  @type index_as_int() :: non_neg_integer()
  @type key() :: term()

  @spec new(index_as_int()) :: %MerkleTree{}
  def new(index) do
    # create directory and a file to store
    path = Path.join(@path, to_string(Node.self()))
    if not File.exists?(path), do: File.mkdir(path)

    file_name =
      index
      |> Integer.to_string()
      |> String.slice(0, 6)

    file_path = Path.join(path, file_name)
    File.rm(file_path)
    File.touch(file_path)

    %MerkleTree{index: index, path: file_path}
  end

  @doc """
  Destroy the merkle tree file in the disk
  """
  @spec destroy(%MerkleTree{}) :: :ok | {:error, :file.posix()}
  def destroy(state) do
    File.rm(state.path)
  end

  @doc """
  Insert (key, object_hash) to merkle tree
  """
  @spec insert(%MerkleTree{}, term(), term()) :: %MerkleTree{}
  def insert(state, key, value) do
    key_hash = :erlang.phash2(key)
    value_hash = :erlang.phash2(value)
    segment = rem(key_hash, state.segments)
    enqueue_action(state, {:put, segment, key, value_hash})
  end

  @doc """
  delete (key, object_hash) in merkle tree
  """
  @spec delete(%MerkleTree{}, term) :: %MerkleTree{}
  def delete(state, key) do
    key_hash = :erlang.phash2(key)
    segment = rem(key_hash, state.segments)
    enqueue_action(state, {:delete, segment, key})
  end

  # Enqueue commands that will be done before updating the tree
  @spec enqueue_action(%MerkleTree{}, term) :: %MerkleTree{}
  defp enqueue_action(state, action) do
    write_buffer = [action | state.write_buffer]
    %{state | write_buffer: write_buffer}
  end

  # Execute all the commands in the write_buffer and save it in the disk
  @spec flush_buffer(%MerkleTree{}) :: {%{integer() => %{}}, %MerkleTree{}}
  defp flush_buffer(state) do
    if not File.exists?(state.path), do: File.touch(state.path)

    data =
      case File.read(state.path) do
        {:ok, ""} ->
          Enum.reduce(0..(state.segments - 1), %{}, fn x, acc -> Map.put(acc, x, %{}) end)

        {:ok, key_map} ->
          :erlang.binary_to_term(key_map)

        {:error, reason} ->
          {:error, reason}
      end

    # IO.puts("#{inspect data}")
    new_data =
      state.write_buffer
      |> List.foldr(
        data,
        fn command, data0 ->
          case command do
            {:put, segment, key, value_hash} ->
              put_in(data0, [segment, key], value_hash)

            {:delete, segment, key} ->
              {_, data0} = pop_in(data0, [segment, key])
              data0
          end
        end
      )
      |> Map.new()

    File.write(state.path, :erlang.term_to_binary(new_data))
    {new_data, %{state | write_buffer: []}}
  end

  @doc """
  Flush the write_buffer and rehash the merkle tree
  """
  @spec update_tree(%MerkleTree{}) :: %MerkleTree{}
  def update_tree(state) do
    {segments, state2} = flush_buffer(state)

    hash_list =
      segments
      |> Enum.map(fn {_k, v} ->
        v |> Enum.map(&:erlang.term_to_binary/1) |> hash
      end)

    # convert to tree node(value, children)
    tree =
      hash_list
      |> Enum.with_index()
      |> Enum.map(fn {hash, segment_num} ->
        %{hash: hash, children: [segment_num]}
      end)

    tree = make_tree(state.levels - 1, tree, hash_list, state.width)
    %{state2 | tree: tree}
  end

  # Build the merkle tree from bottom up
  @spec make_tree(integer(), %{{integer(), integer()} => binary()}, [binary()], %MerkleTree{}) ::
          binary()
  defp make_tree(-1, [tree], [_top_hash], _width) do
    tree
  end

  defp make_tree(level, tree, hash_list, width) do
    # IO.puts inspect hash_list
    children = Enum.chunk_every(tree, width)
    # making parent hash from child nodes
    child_hashes =
      hash_list
      |> Enum.chunk_every(width)
      |> Enum.map(&hash/1)

    # this is making a merkle tree node with hash and children as its element
    tree =
      Enum.zip(child_hashes, children)
      |> Enum.map(fn {h, c} -> %{hash: h, children: c} end)

    make_tree(level - 1, tree, child_hashes, width)
  end

  @spec hash([binary()]) :: binary()
  defp hash(binary_list) do
    context = :crypto.hash_init(:sha)

    binary_list
    |> Enum.reduce(
      context,
      fn binary, context0 ->
        :crypto.hash_update(context0, binary)
      end
    )
    |> :crypto.hash_final()
  end

  @doc """
  Compare two merkle trees in dfs and return list of different segments
  """
  def compare_trees({segment, segment}) when is_integer(segment) do
    segment
  end

  @spec compare_trees({%{}, %{}}) :: [integer()]
  def compare_trees({my_tree, other_tree}) do
    case my_tree.hash == other_tree.hash do
      true ->
        []

      false ->
        Enum.zip(my_tree.children, other_tree.children)
        |> Enum.map(&compare_trees/1)
        |> List.flatten()
    end
  end

  @doc """
  Return a merged map of segments from the file in the path
  """
  @spec get_segments(%MerkleTree{}, [integer()]) :: %{key() => binary()} | {:error, term()}
  def get_segments(state, segment_list) do
    case File.read(state.path) do
      {:ok, segments} ->
        :erlang.binary_to_term(segments)
        |> Map.take(segment_list)
        |> Map.values()
        |> Enum.reduce(%{}, fn segment, acc ->
          Map.merge(acc, segment)
        end)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Return a merged map of segments from the file in the path
  """
  @spec compare_segments(%{}, %{}) :: [{:different | :missing | :other_missing, key()}]
  def compare_segments(my_segments, other_segments) do
    my_key_set = Map.keys(my_segments) |> MapSet.new()
    my_set = MapSet.new(my_segments)

    other_key_set = Map.keys(other_segments) |> MapSet.new()
    other_set = MapSet.new(other_segments)

    intersection =
      my_set
      |> MapSet.intersection(other_set)
      |> Map.new()
      |> Map.keys()
      |> MapSet.new()

    key_intersection = MapSet.intersection(my_key_set, other_key_set)
    difference = MapSet.difference(key_intersection, intersection) |> Enum.map(&{:different, &1})

    me_missing = MapSet.difference(other_key_set, my_key_set) |> Enum.map(&{:missing, &1})

    other_missing =
      MapSet.difference(my_key_set, other_key_set) |> Enum.map(&{:other_missing, &1})

    difference ++ me_missing ++ other_missing
  end
end
