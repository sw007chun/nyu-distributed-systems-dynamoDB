defmodule ActiveAntiEntropy do
  @moduledoc """
  Active Anti-entropy using merkle tree

  Note that this only works after every nodes have joined in the cluster.
  """
  use GenServer
  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  def insert(key, value, index) do
    GenServer.cast(__MODULE__, {:insert, key, value, index})
  end

  def get_tree(index) do
    GenServer.call(__MODULE__, {:get_tree, index})
  end

  def get_segments(index, segment_list) do
    GenServer.call(__MODULE__, {:get_segments, index, segment_list})
  end

  def start_aae() do
    GenServer.cast(__MODULE__, :start_aae)
  end

  @aae_freq 1_000

  @impl true
  def init(:ok) do
    {:ok, ring} = Ring.Manager.get_my_ring()
    replication = Application.get_env(:dynamo, :replication)
    state = %{replication: replication}

    state =
      Ring.all_indices(ring)
      |> Enum.reduce(state,
      fn index, state0 ->
        Map.put(state0, index, MerkleTree.new(index))
      end)

    {:ok, state}
  end

  @impl true
  def handle_cast({:insert, key, value, index}, state) do
    state =
      Map.update!(state, index, fn tree ->
        MerkleTree.insert(tree, key, value)
      end)
    {:noreply, state}
  end

  @impl true
  def handle_call({:get_tree, index}, _from, state) do
    {tree, state} =
      Map.get_and_update!(state, index, fn tree ->
      tree = MerkleTree.update_tree(tree)
      {tree, tree}
    end)
    {:reply, tree, state}
  end

  @impl true
  def handle_call({:get_segments, index, segment_list}, _from, state) do
    segments =
      Map.get(state, index)
      |> MerkleTree.get_segments(segment_list)
    {:reply, segments, state}
  end

  @impl true
  def handle_cast(:start_aae, state) do
    {:ok, ring} = Ring.Manager.get_my_ring()
    my_indices = Ring.my_indices(ring)

    for index <- my_indices do
      pref_list = Ring.Manager.get_self_exclusive_pref_list(index, state.replication - 1)
      my = state |> Map.get(index) |> MerkleTree.update_tree

      for {_i, node} <- pref_list do
        other = GenServer.call({__MODULE__, node}, {:get_tree, index})
        comparison = MerkleTree.compare_trees({my.tree, other.tree})

        if not Enum.empty?(comparison) do
          Logger.info("Comparsion: #{Enum.join(comparison, ", ")}")
          my_segments = MerkleTree.get_segments(my, comparison)
          other_segments = GenServer.call({__MODULE__, node}, {:get_segments, index, comparison})

          MerkleTree.compare_segments(my_segments, other_segments)
          |> Enum.map(fn {status, key} ->
            Logger.info("#{inspect status}: #{inspect key}")
            Logger.info inspect KV.get(key)
          end)
        end
      end
    end

    # This is to prevent live lock
    member_list = Ring.active_members(ring)
    next_node =
      member_list ++ member_list
      |> Enum.drop_while(fn node -> node != Node.self() end)
      |> Enum.at(1)

    Dynamo.TaskSupervisor
    |> Task.Supervisor.start_child(fn ->
      Process.sleep(@aae_freq)
      GenServer.cast({__MODULE__, next_node}, :start_aae)
    end)

    {:noreply, state}
  end
end
