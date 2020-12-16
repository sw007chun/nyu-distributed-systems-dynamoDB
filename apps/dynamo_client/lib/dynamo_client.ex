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

  def get_membership_info do
    GenServer.call(__MODULE__, :get_membership_info)
  end

  def clear_storage do
    GenServer.call(__MODULE__, :clear_storage)
  end

  def reset_vnode_param(param_list) do
    GenServer.call(__MODULE__, {:reset_vnode_param, param_list})
  end

  def reset_client_param(param_list) do
    GenServer.call(__MODULE__, {:reset_client_param, param_list})
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

  @spec pref_list(term(), non_neg_integer(), %CHash{}) :: term()
  def pref_list(key, n_val, membership) do
    index = CHash.hash_of(key)
    CHash.successors(index, n_val, membership)
  end

  @impl true
  def init(:ok) do
    Logger.info("client started")
    replication = Application.get_env(:dynamo, :replication)
    read = Application.get_env(:dynamo, :R)
    write = Application.get_env(:dynamo, :W)
    read_repair = Application.get_env(:dynamo, :read_repair)

    {:ok,
     %{
       node_list: [],
       replication: replication,
       read: read,
       write: write,
       read_repair: read_repair,
       membership_ring: nil
     }}
  end

  @impl true
  def handle_call({:connect, node}, _from, state) do
    connected? = Node.connect(node)
    node_list = Node.list()
    membership_ring = GenServer.call({DynamoServer, node}, :get_membership_info)
    {:reply, connected?, %{state | node_list: node_list, membership_ring: membership_ring}}
  end

  @impl true
  def handle_call(:get_membership_info, _from, state) do
    node = Enum.random(state.node_list)
    membership_ring = GenServer.call({DynamoServer, node}, :get_membership_info)
    {:reply, :ok, %{state | membership_ring: membership_ring}}
  end

  @impl true
  def handle_call(:clear_storage, _from, state) do
    state.node_list
    |> Enum.map(fn node ->
      GenServer.call({DynamoServer, node}, :clear_storage) end)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:reset_vnode_param, param_list} , _from, state) do
    state.node_list
    |> Enum.map(fn node ->
      GenServer.call({DynamoServer, node}, {:reset_vnode_param, param_list}) end)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:reset_client_param, param_list} , _from, state) do
    state =
      param_list
      |> Enum.reduce(state,
      fn {key, value}, state0 -> %{state0 | key => value} end)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:put, key, value, context}, _from, state) do
    node =
      if state.membership_ring do
        [{_, top_node} | _] = pref_list(key, 1, state.membership_ring)
        top_node
      else
        Enum.random(state.node_list)
      end
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
