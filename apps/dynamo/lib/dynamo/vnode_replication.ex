defmodule Vnode.Replication do
  @moduledoc """
  This module stores the replication logics for put operation and
  read repair logic for get operation.
  """
  require Logger

  @type node_name() :: atom()
  @type vclock() :: map()

  @doc """
  Replicate operations to following vnodes
  """
  def replicate_put(key, value, context, state, return_pid) do
    my_index = state.partition
    pref_list = Ring.Manager.get_preference_list(my_index - 1, state.replication)

    Logger.info("Sending W replies")
    # send asynchronous replication task to other vnodes
    for {index, node} <- pref_list do
      GenServer.cast(
        {Vnode.Master, node},
        {:command, index,
          {:put_repl, key, value, context, return_pid, false}}
      )
    end
  end

  def get_reponses(key, state, return_pid) do
    pref_list =
      CHash.hash_of(key)
      |> Ring.Manager.get_preference_list(state.replication)
    [{partition_index, _} | _] = pref_list
    Logger.info("Waiting for R replies")

    # send asynchronous replication task to other vnodes
    for {index, node} <- pref_list do
      GenServer.cast(
        {Vnode.Master, node},
        {:command, index,
          {:get_repl, return_pid, partition_index, key, true}}
      )
    end
  end
end
