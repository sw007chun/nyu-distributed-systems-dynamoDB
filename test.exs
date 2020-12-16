defmodule Test do
  def test(current, goal, count, delay) do
    if current < goal do
      :ok = DynamoClient.put(current, current)
      Process.sleep(delay)
      value = DynamoClient.get(current)
      if value == [current] do
        test(current + 1, goal, count + 1, delay)
      else
        test(current + 1, goal, count, delay)
      end
    else
      count / goal
    end
  end

  def now() do
    {mega, seconds, ms} = :os.timestamp()
    mega * 1_000_000 + seconds
  end
end

foo = :"foo@127.0.0.1"
bar = :"bar@127.0.0.1"
baz = :"baz@127.0.0.1"
panda = :"panda@127.0.0.1"

:pong = Node.ping(foo)
:pong = Node.ping(bar)
:pong = Node.ping(baz)
:pong = Node.ping(panda)

GenServer.call({DynamoServer, bar}, {:join, foo})
GenServer.call({DynamoServer, baz}, {:join, bar})
GenServer.call({DynamoServer, panda}, {:join, baz})
Process.sleep(1_000)

DynamoClient.connect(foo)

read_writes = [[read: 1, write: 1], [read: 1, write: 2], [read: 2, write: 1]]
num_tries = 1000
time_intervals = :lists.seq(0, 10, 1)
start = Test.now()

read_write_result =
  for params <- read_writes do
    DynamoClient.reset_client_param(params)
    for i <- time_intervals do
      IO.puts "RW Time after write: #{i} msec"
      result = Test.test(0, num_tries, 0, i)
      DynamoClient.clear_storage
      IO.puts result
      result
    end
  end

replications = [[replication: 2], [replication: 3], [replication: 4]]
DynamoClient.reset_client_param([read: 1, write: 1])

n_result =
  for params <- replications do
    DynamoClient.reset_vnode_param(params)
    for i <- time_intervals do
      IO.puts "N Time after write: #{i} msec"
      result = Test.test(0, num_tries, 0, i)
      DynamoClient.clear_storage
      IO.puts result
      result
    end
  end

time = Test.now() - start

for {read_write, consistency} <- Enum.zip(read_writes, read_write_result) do
  IO.puts "Read=#{Keyword.get(read_write, :read)}, Write=#{Keyword.get(read_write, :write)}"
  for {i, value} <- Enum.zip(time_intervals, consistency) do
    IO.puts "#{i} : #{value}"
  end
  IO.puts ""
end

for {replication, consistency} <- Enum.zip(replications, n_result) do
  IO.puts "N=#{Keyword.get(replication, :replication)} Replications"
  for {i, value} <- Enum.zip(time_intervals, consistency) do
    IO.puts "#{i} : #{value}"
  end
  IO.puts ""
end

IO.puts "Time taken: #{div(time, 60)}:#{rem(time, 60)}"
