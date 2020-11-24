defmodule DynamoNode do
  @moduledoc """
  This module defines the dynamo node and the requests it serves
  """

  @doc """
  Function to call on starting the node
  """
  @spec init :: nil
  def init() do
    # TODO
    # Write the initialization code
    ring = CHash.new_ring(5, :test_node)
    start(ring)
  end

  @doc """
  Function to handle all the requests coming to the node
  get and put are basic client requests
  get_all is used to get value from all nodes in preference list
  put_all is used to replicate the key-value in N nodes
  """
  @spec start(%CHash{}) :: nil
  def start(ring) do
    # TODO
    # Write code to handle each request
    receive do
      {_sender, {:get, {key, value}}} ->
        send_to_coordinator(key, value, :get_all, ring)
      {_sender, {:put, {key, value}}} ->
        send_to_coordinator(key, value, :put_all, ring)
      {_sender, {:get_all, value}} ->
        nil
      {_sender, {:put_all, value}} ->
        nil
    end
  end

  @doc """
  Function to send the key-value to the coordinator node
  """
  @spec send_to_coordinator(any(), any(), :get_all|:put_all, %CHash{}) :: nil
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

end
