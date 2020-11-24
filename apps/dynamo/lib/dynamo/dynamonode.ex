defmodule DynamoNode do
  @moduledoc """
  This module defines the dynamo node and the requests it serves
  """

  import Emulation, only: [send: 2, whoami: 0]

  import Kernel, except: [send: 2]

  defstruct(
    # Number of replications
    N: 3,
    # The consistent hashing ring
    ring: nil,
    # The persistent storage used by this node
    storage: nil
  )

  @doc """
  Function to call on starting the node
  """
  @spec init(integer()) :: nil
  def init(num_replica) do
    state = %DynamoNode{
      N: num_replica,
      ring: CHash.new_ring(5, :test_node),
      storage: PersistentStore.init_store()
    }
    start(state)
  end

  @doc """
  Function to handle all the requests coming to the node
  get and put are basic client requests
  get_all is used to get value from all nodes in preference list
  put_all is used to replicate the key-value in N nodes
  """
  @spec start(%DynamoNode{}) :: nil
  def start(state) do
    # TODO
    # Write code to handle each request
    receive do
      {_sender, {:get, {key, value}}} ->
        send_to_coordinator(key, value, :get_all, state.ring)
        start(state)

      {_sender, {:put, {key, value}}} ->
        send_to_coordinator(key, value, :put_all, state.ring)
        start(state)

      {_sender, {:get_all, value}} ->
        start(state)

      {_sender, {:put_all, {key, value}}} ->
        # Make sure this node is the coordinator node
        # If true store and replicate
        # Else send to real coordinator
        {flag, index, node} = check_coordinator(key, state.ring)
        if flag do
          state = store(key, value,  index, state)
          start(state)
        else
          send(node, {:put_all, {key, value}})
          start(state)
        end

      # Call to replicate the key-value
      # Call should be made by the coordinator of the key
      {_sender, {:repl, {key, value}}} ->
        state = %{state | storage: PersistentStore.put_value(key, value, state.storage)}
        start(state)
    end
  end

  @doc """
  Function to send the key-value to the coordinator node
  """
  @spec send_to_coordinator(any(), any(), :get_all|:put_all, %CHash{}) :: any()
  def send_to_coordinator(key, value, message, chash) do
    coordinator_node = get_coordinator_node(key, chash)
    send(coordinator_node, {message, {key, value}})
  end

  @doc """
  This function gets the N nodes used for replication.
  It uses the hash of the key in to lookup the coordinator node
  """
  @spec get_coordinator_node(any(), %CHash{}) :: term()
  def get_coordinator_node(key, chash) do
    <<n::160>> = CHash.key_of(key)
    index = CHash.next_index(n, chash)
    CHash.lookup(index, chash)
  end

  @doc """
  Function to make sure the node itself is the coordinator node
  """
  @spec check_coordinator(any(), %CHash{}) :: {true|false, integer(), term()}
  def check_coordinator(key, chash) do
    <<n::160>> = CHash.key_of(key)
    index = CHash.next_index(n, chash)
    node = CHash.lookup(index, chash)
    if node == whoami() do
      {true, index, node}
    else
      {false, index, node}
    end
  end

  @doc """
  Function to store and replicate key-value
  """
  @spec store(any(), any(), integer(), %DynamoNode{}) :: %DynamoNode{}
  def store(key, value, index, state) do
    state = %{state | storage: PersistentStore.put_value(key, value, state.storage)}
    replicate(key, value, index, state)
    state
  end

  @doc """
  Function to replicate key-value in N-1 nodes
  """
  @spec replicate(any(), any(), integer(), %DynamoNode{}) :: [any]
  def replicate(key, value, index, state) do
    repl_factor = Map.get(state, :N, 0)
    num_nodes = CHash.size(state.ring)
    for i <- :lists.seq(index+1, index+repl_factor, 1) do
      node = CHash.lookup(rem(i, num_nodes), state.ring)
      send(node, {:repl, {key, value}})
    end
  end
end
