defmodule Dynamo do
  @moduledoc """
  Documentation for `Dynamo`.
  """
  require Logger
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
        Logger.info("Joining #{other_node} from #{my_node}")
        # Add a new member to the ring that wants to be joined
        # and set that ring to be my ring
        # and send that ring to the owner node of the ring
        Ring.add_member(my_node, other_ring, my_node)
        |> Ring.set_owner(my_node)
        |> Ring.Manager.set_my_ring()
        |> Ring.Gossip.send_ring(other_node)
    end
  end

  def leave do
    my_node = Node.self()
    {:ok, my_ring} = Ring.Manager.get_my_ring()

    case {Ring.all_members(my_ring), Ring.member_status(my_ring, my_node)} do
      {_, :invalid} ->
        {:error, :not_member}

      {[^my_node], _} ->
        {:error, :only_member}

      {_, :valid} ->
        leave(my_node)

      {_, _} ->
        {:error, :already_leaving}
    end
  end

  @spec leave(node()) :: none()
  def leave(node) do
    _fresh_ring =
      Ring.Manager.ring_transform(
        fn my_ring, _ ->
          new_ring = Ring.leave_member(node, my_ring, node)
          {:new_ring, new_ring}
        end,
        []
      )

    Logger.info("#{node} has left the cluster")
    :ok
  end
end
