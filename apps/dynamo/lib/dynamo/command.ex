defmodule KVServer.Command do
  @doc """
  This module is from
  https://elixir-lang.org/getting-started/mix-otp/docs-tests-and-with.html#running-commands

  Slightly modified for dynamo use
  """
  require Logger

  def run(command)

  def run({:join, node}) do
    node
    |> String.to_atom()
    |> Dynamo.join()

    {:ok, "Joining #{inspect(node)} \n"}
  end

  def run(:leave) do
    Dynamo.leave()
    {:ok, "Leaving cluster\n"}
  end

  def run({:stabilize, key}) do
    time = KV.stabilize(key)
    {:ok, "#{time}\n"}
  end

  def run(:ring_status) do
    ring = KV.ring_status()
    {:ok, ring <> "\n"}
  end

  def run({:get, key}) do
    value = KV.get(key)
    {:ok, "#{value}\nOK\n"}
  end

  def run({:put, key, value}) do
    KV.put(key, value)
    {:ok, "OK\n"}
  end

  def run({:delete, key}) do
    KV.delete(key)
    {:ok, "OK\n"}
  end

  def parse(line) do
    case String.split(line) do
      ["JOIN", node] -> {:ok, {:join, node}}
      ["LEAVE"] -> {:ok, :leave}
      ["STABILIZE", key] -> {:ok, {:stabilize, key}}
      ["RING_STATUS"] -> {:ok, :ring_status}
      ["GET", key] -> {:ok, {:get, key}}
      ["PUT", key, value] -> {:ok, {:put, key, value}}
      ["DELETE", key] -> {:ok, {:delete, key}}
      _ -> {:error, :unknown_command}
    end
  end
end
