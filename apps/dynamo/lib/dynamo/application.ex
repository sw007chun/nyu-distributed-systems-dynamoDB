defmodule Dynamo.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: Registry.Vnode},
      {DynamicSupervisor, name: Vnode.Supervisor, strategy: :one_for_one},
      {Task.Supervisor, name: Dynamo.TaskSupervisor},
      {Ring.Manager, name: Ring.Manager},
      {Ring.Gossip, name: Ring.Gossip},
      {Vnode.Master, name: Vnode.Master},
      {ActiveAntiEntropy, name: ActiveAntiEntropy},
      {DynamoServer, name: DynamoServer}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_all, name: Dynamo.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
