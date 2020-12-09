# start the current node as a manager
:ok = LocalCluster.start()

exclude = if Node.alive?, do: [], else: [distributed: true]

# start your application tree manually
Application.ensure_all_started(:dynamo)

ExUnit.start(exclude: exclude)
