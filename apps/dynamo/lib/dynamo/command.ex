defmodule KVServer.Command do
  @doc """
  This module is from
  https://elixir-lang.org/getting-started/mix-otp/docs-tests-and-with.html#running-commands

  Slightly modified for dynamo use
  """
  def run(command)

  def run({:join, node}) do
    node
    |> String.to_atom()
    |> Dynamo.join()

    {:ok, "Joining #{inspect(node)} \r\n"}
  end

  def run(:leave) do
    Dynamo.leave()
    {:ok, "Leaving cluster\r\n"}
  end

  def run(:ring_status) do
    ring = KV.ring_status()
    {:ok, ring <> "\n"}
  end

  def run({:get, key}) do
    value = KV.get(key)
    {:ok, "#{value}\r\nOK\r\n"}
  end

  def run({:put, key, value}) do
    KV.put(key, value)
    {:ok, "OK\r\n"}
  end

  def run({:delete, key}) do
    KV.delete(key)
    {:ok, "OK\r\n"}
  end

  def parse(line) do
    case String.split(line) do
      ["JOIN", node] -> {:ok, {:join, node}}
      ["LEAVE"] -> {:ok, :leave}
      ["RING_STATUS"] -> {:ok, :ring_status}
      ["GET", key] -> {:ok, {:get, key}}
      ["PUT", key, value] -> {:ok, {:put, key, value}}
      ["DELETE", key] -> {:ok, {:delete, key}}
      _ -> {:error, :unknown_command}
    end
  end
end
