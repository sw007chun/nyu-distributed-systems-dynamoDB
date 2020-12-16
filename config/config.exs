# This file is responsible for configuring your umbrella
# and **all applications** and their dependencies with the
# help of the Config module.
#
# Note that all applications in your umbrella share the
# same configuration and dependencies, which is why they
# all use the same configuration file. If you want different
# configurations or dependencies per app, it is best to
# move said applications out of the umbrella.
import Config

# Sample configuration:
#
#     config :logger, :console,
#       level: :info,
#       format: "$date $time [$level] $metadata$message\n",
#       metadata: [:user_id]
#

case Mix.env() do
  env when env in [:dev, :prod] ->
    config :dynamo,
      ring_size: 16,
      read_repair: false,
      aae: false,
      replication: 3,
      R: 1,
      W: 1,
      aae_freq: 1_000,
      write_delay: 3,
      ars_delay: 0.25

    config :logger, :console,
      level: :info

  :test ->
    config :dynamo,
      ring_size: 8,
      read_repair: true,
      aae: true,
      replication: 3,
      R: 2,
      W: 2,
      aae_freq: 1_000,
      write_delay: 0,
      ars_delay: 0
end
