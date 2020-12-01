defmodule Dynamo do
  @moduledoc """
  Documentation for `Dynamo`.
  """

  @type ring() :: Ring

  @doc """
  Return the ring from the remote node
  """
  @spec get_other_ring(atom()) :: ring()
  def get_other_ring(node) do
    {Dynamo.TaskSupervisor, node}
    |> Task.Supervisor.async(Ring.Manager, :get_my_ring, [])
    |> Task.await()
  end

  @doc """
  Join into another node
  """
  @spec join(atom()) :: :ok | {:error, atom()}
  def join(other_node) when is_atom(other_node) do
    case Node.ping(other_node) do
      :pong ->
        case get_other_ring(other_node) do
          {:ok, other_ring} ->
            join(other_node, other_ring)
          _ ->
            {:error, :unable_to_get_ring}
        end
      :pang ->
        {:error, :not_reachable}
    end
  end

  @spec join(atom(), ring()) :: :ok | {:error, atom()}
  defp join(other_node, other_ring) do
    my_node = Node.self()
    {:ok, my_ring} = Ring.Manager.get_my_ring()
    same_size = Ring.ring_size(my_ring) == Ring.ring_size(other_ring)
    # Only a new node with itself in their ring can join the cluster
    singleton = [my_node] === Ring.all_members(my_ring)

    case {same_size, singleton} do
      {false, _} ->
        {:error, :different_ring_size}
      {_, false} ->
        {:error, :not_single_node}
      _ ->
        IO.puts "Joining #{other_node} from #{my_node}"
        # Add a new member to the ring that wants to be joined
        # and set that ring to be my ring
        # and send that ring to the owner node of the ring
        Ring.add_member(my_node, other_ring, my_node)
        |> Ring.set_owner(my_node)
        |> Ring.Manager.set_my_ring()
        |> Ring.Gossip.send_ring(other_node)
    end
  end
end
