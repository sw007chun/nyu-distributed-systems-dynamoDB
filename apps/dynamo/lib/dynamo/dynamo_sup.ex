defmodule Dynamo.Supervisor do
  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, :ok, opts)
  end

  @impl true
  def init(:ok) do
    children = [
      {DynamicSupervisor, name: Vnode.Supervisor, strategy: :one_for_one},
      {Task.Supervisor, name: Dynamo.TaskSupervisor},
      {Ring.Manager, name: Ring.Manager},
      {Vnode.Manager, name: Vnode.Manager},
      {Ring.Gossip, name: Ring.Gossip},
      {Vnode.Master, name: Vnode.Master}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end