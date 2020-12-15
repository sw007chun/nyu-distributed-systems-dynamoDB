defmodule DynamoClientTest do
  use ExUnit.Case

  setup do
    Application.stop(:dynamo_client)
    :ok = Application.start(:dynamo_client)
  end

  @tag :distributed
  test "basic" do
    nodes = LocalCluster.start_nodes("node-", 3, [
      applications: [:dynamo]
    ])

    [node1, node2, node3] = nodes

    GenServer.call({DynamoServer, node2}, {:join, node1})
    GenServer.call({DynamoServer, node3}, {:join, node1})
    Process.sleep(1_000)

    assert DynamoClient.connect(node1) == true
    assert DynamoClient.put(:hello, "world") == :ok
    assert DynamoClient.get(:hello) == ["world"]

    :ok = GenServer.call({DynamoServer, node1}, {:put_single, :hello, "foo"})
    count = read_repair(:hello, "foo", 1)
    IO.puts("Read repaired in #{count} get requests")

    :ok = GenServer.call({DynamoServer, node1}, {:put_single, :hello, "AAE"})
    assert GenServer.call({DynamoServer, node1}, :start_aae) == "AAE started"
    Process.sleep(3_000)

    assert DynamoClient.get(:hello) == ["AAE"]
  end

  def read_repair(key, value, count) do
    [return_value] = DynamoClient.get(key)
    if return_value == value do
      count
    else
      read_repair(key, value, count + 1)
    end
  end
end
