defmodule Dynamo.Supervisor do
  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, :ok, opts)
  end

  @impl true
  def init(:ok) do
    port =
      String.to_integer(System.get_env("PORT") || Application.get_env(:dynamo, :port) || "4040")

    children = [
      {Registry, keys: :unique, name: Registry.Vnode},
      {DynamicSupervisor, name: Vnode.Supervisor, strategy: :one_for_one},
      {Task.Supervisor, name: Dynamo.TaskSupervisor},
      {Ring.Manager, name: Ring.Manager},
      {Ring.Gossip, name: Ring.Gossip},
      {Vnode.Master, name: Vnode.Master},
      {ActiveAntiEntropy, name: ActiveAntiEntropy},
      Supervisor.child_spec({Task, fn -> KVServer.accept(port) end}, restart: :permanent)
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end
