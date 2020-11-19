defmodule DynamoNode do
  @moduledoc """
  This module defines the dynamo node and the requests it serves
  """

  @doc """
  Function to call on starting the node
  TODO
  Write the initialization code
  """
  @spec init :: nil
  def init() do
    ring = CHash.new_ring(5, :test_node)
    start(ring)
  end

  @doc """
  Function to handle all the requests coming to the node
  TODO
  Write code to handle each request
  """
  @spec start(%CHash{}) :: nil
  def start(ring) do
    receive do
      {:get, value} ->
        nil
      {:put, value} ->
        nil
      {:replicate, value} ->
        nil
    end

  end

  @doc """
  Function to send the key-value to the coordinator node
  """
  @spec send_to_coordinator(any(), any(), %CHash{}) :: nil
  def send_to_coordinator(key, value, chash) do
    coordinator_node = get_coordinator_node(key, chash)
    send(coordinator_node, {:replicate, {key, value}})
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
