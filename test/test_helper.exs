# Redis integration tests need a real Redis. Point SOCKETELX_TEST_REDIS_URL at one
# (CI does; locally: `docker run -p 6379:6379 redis:7` and
# `SOCKETELX_TEST_REDIS_URL=redis://localhost:6379/15 mix test`).
# They are skipped otherwise, so `mix test` always works with no services.
#
# The peer tests start real separate Erlang nodes, which needs this VM to be
# distributed (epmd running). If that can't be arranged they are skipped.
distributed? =
  Node.alive?() or
    try do
      System.cmd("epmd", ["-daemon"], stderr_to_stdout: true)
      match?({:ok, _}, Node.start(:"socketelx_test@127.0.0.1", :longnames))
    rescue
      _ -> false
    catch
      _, _ -> false
    end

# The :redis_cluster tests also need SOCKETELX_TEST_REDIS_CLUSTER=true and a URL that
# points at a node of a real Redis Cluster (at least 3 masters, each with a replica).
exclude =
  if(System.get_env("SOCKETELX_TEST_REDIS_URL"), do: [], else: [:redis]) ++
    if(System.get_env("SOCKETELX_TEST_REDIS_CLUSTER") in ["true", "1"],
      do: [],
      else: [:redis_cluster]
    ) ++
    if(distributed?, do: [], else: [:peer])

ExUnit.start(exclude: exclude)
