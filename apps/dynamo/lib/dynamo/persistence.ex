defmodule PersistentStore do
  @moduledoc """
  This module defines the APIs used for storage for by a node.
  Uses an in-memory map to store key value pairs.
  """
  defstruct(key_value_store: %{})

  @doc """
  Initialize the store, when starting a node
  """
  @spec init_store() :: %PersistentStore{}
  def init_store() do
    %PersistentStore{key_value_store: %{}}
  end

  @doc """
  Put a key-value pair in the store
  """
  @spec put_value(any(), any(), %PersistentStore{}) :: %PersistentStore{}
  def put_value(key, value, store) do
    %{store | key_value_store: Map.put(store.key_value_store, key, value)}
  end

  @doc """
  Get the value associated with a key from the store
  """
  @spec get_value(any(), %PersistentStore{}) :: any()
  def get_value(key, store) do
    Map.get(store.key_value_store, key)
  end
end
