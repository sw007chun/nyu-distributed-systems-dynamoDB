defmodule DynamoClient do
  @moduledoc """
  Client application for Dynamo.
  Client coordinates get and put operation directly.
  """

  use GenServer
  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  def connect(node) do
    GenServer.call(__MODULE__, {:connect, node})
  end

  def put(key, value, context \\ :no_context) do
    GenServer.call(__MODULE__, {:put, key, value, context})
  end

  # Deleting is just putting empty value to the key.
  def delete(key, context \\ :no_context) do
    GenServer.call(__MODULE__, {:put, key, [], context})
  end

  def get(key) do
    GenServer.call(__MODULE__, {:get, key})
  end

  @impl true
  def init(:ok) do
    Logger.info "client started"
    replication = Application.get_env(:dynamo, :replication)
    read = Application.get_env(:dynamo, :R)
    write = Application.get_env(:dynamo, :W)
    read_repair = Application.get_env(:dynamo, :read_repair)

    {:ok, %{node_list: [], replication: replication, read: read, write: write, read_repair: read_repair}}
  end

  @impl true
  def handle_call({:connect, node}, _from, state) do
    connected? = Node.connect(node)
    node_list = Node.list()
    {:reply, connected?, %{state | node_list: node_list}}
  end

  @impl true
  def handle_call({:put, key, value, context}, _from, state) do
    # TODO: keep own list of preference list
    node = Enum.random(state.node_list)
    task =
      Client.TaskSupervisor
      |> Task.Supervisor.async(fn ->
        Coordination.wait_write_response(0, state.write)
      end)

    GenServer.cast({DynamoServer, node}, {:put, key, value, context, task.pid})
    {:reply, Task.await(task, :infinity), state}
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    node = Enum.random(state.node_list)
    # Spawn task to wait for R reponses from other vnodes
    task =
      Dynamo.TaskSupervisor
      |> Task.Supervisor.async(fn ->
        Coordination.get_reponses(key, node, state, Client.TaskSupervisor)
      end)

    value = Task.await(task, :infinity)
    {:reply, value, state}
  end
end
