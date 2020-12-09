defmodule DynamoTest do
  use ExUnit.Case

  # test "something with a required cluster" do
  #   nodes = LocalCluster.start_nodes("node-", 3, [
  # files: [
  #   __ENV__.file
  # ],
  #   ])

  #   [node1, node2, node3] = nodes
  #   pid = self()
  #   Node.spawn(node2, fn -> Dynamo.join(:"node-1@127.0.0.1") end)
  #   Node.spawn(node3, fn -> Dynamo.join(:"node-2@127.0.0.1") end)
  #   Node.spawn(node1, fn -> send(pid, KV.put(:hello, "world")) end)

  #   assert_receive :ok

  #   Node.spawn(node1, fn -> send(pid, KV.get(:hello)) end)
  #   Node.spawn(node2, fn -> send(pid, KV.get(:hello)) end)
  #   Node.spawn(node3, fn -> send(pid, KV.get(:hello)) end)

  #   assert_receive "world"
  #   assert_receive "world"
  #   assert_receive "world"

  # end

  @tag capture_log: false
  test "tcp connection" do
    [replication, read, write] = [3, 2, 2]
    [port1, port2, port3] = [4041, 4042, 4043]

    [node1] =
      LocalCluster.start_nodes("node-1", 1,
        files: [__ENV__.file],
        environment: [
          dynamo: [
            port: Integer.to_string(port1),
            replication: replication,
            R: read,
            W: write
          ]
        ]
      )

    [node2] =
      LocalCluster.start_nodes("node-2", 1,
        files: [__ENV__.file],
        environment: [
          dynamo: [
            port: Integer.to_string(port2),
            replication: replication,
            R: read,
            W: write
          ]
        ]
      )

    [node3] =
      LocalCluster.start_nodes("node-3", 1,
        files: [__ENV__.file],
        environment: [
          dynamo: [
            port: Integer.to_string(port3),
            replication: replication,
            R: read,
            W: write
          ]
        ]
      )

    {:ok, socket1} = :gen_tcp.connect(:localhost, port1, [:binary, active: false])
    {:ok, socket2} = :gen_tcp.connect(:localhost, port2, [:binary, active: false])
    {:ok, socket3} = :gen_tcp.connect(:localhost, port3, [:binary, active: false])

    # joining cluster by joining any node in the cluster
    :gen_tcp.send(socket2, "JOIN node-11@127.0.0.1\n")
    :gen_tcp.send(socket3, "JOIN node-21@127.0.0.1\n")

    assert :gen_tcp.recv(socket2, 0) == {:ok, "Joining \"node-11@127.0.0.1\" \n"}
    assert :gen_tcp.recv(socket3, 0) == {:ok, "Joining \"node-21@127.0.0.1\" \n"}
    Process.sleep(100)

    # put (hello, world) into the dynamo cluster
    :gen_tcp.send(socket1, "PUT hello world\n")
    assert :gen_tcp.recv(socket1, 0) == {:ok, "OK\n"}
    Process.sleep(10)

    # retrieving same value from any node in the cluster
    :gen_tcp.send(socket1, "GET hello\n")
    assert :gen_tcp.recv(socket1, 0) == {:ok, "world\nOK\n"}
    :gen_tcp.send(socket2, "GET hello\n")
    assert :gen_tcp.recv(socket2, 0) == {:ok, "world\nOK\n"}
    :gen_tcp.send(socket3, "GET hello\n")
    assert :gen_tcp.recv(socket3, 0) == {:ok, "world\nOK\n"}

    Node.spawn(node2, fn ->
      KV.put_single(
        "hello",
        "baz",
        548_063_113_999_088_594_326_381_812_268_606_132_370_974_703_616
      )
    end)

    Process.sleep(100)
    :gen_tcp.send(socket2, "STABILIZE hello\n")
    # receive do
    #   a -> IO.puts a
    # end

    IO.puts(inspect(:gen_tcp.recv(socket2, 0)))
  end
end
