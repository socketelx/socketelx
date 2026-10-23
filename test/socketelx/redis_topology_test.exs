defmodule Socketelx.RedisTopologyTest do
  use ExUnit.Case, async: true
  alias Socketelx.Backend.Redis.Topology

  # Values checked against `redis-cli cluster keyslot`.
  test "key_slot matches Redis" do
    assert Topology.key_slot("123456789") == 12_739
    assert Topology.key_slot("foo") == 12_182
    assert Topology.key_slot("{socketelx:app-id}:x") == 10_943
  end

  test "only the hash tag counts" do
    assert Topology.key_slot("{user1000}.following") == Topology.key_slot("{user1000}.followers")
    assert Topology.key_slot("{user1000}.following") == Topology.key_slot("user1000")
    # empty tag: the whole key is hashed
    assert Topology.key_slot("foo{}{bar}") != Topology.key_slot("bar")
    # first { and the first } after it
    assert Topology.key_slot("foo{{bar}}zap") == Topology.key_slot("{bar")
    assert Topology.key_slot("foo{bar}{zap}") == Topology.key_slot("bar")
  end

  test "all of an app's keys share one slot in cluster mode" do
    config = Socketelx.Config.resolve(:topology_keys, redis_url: "redis://x", redis_cluster: true)
    assert config.prefix == "{socketelx:app-id}"

    keys = [
      "#{config.prefix}:subs:room",
      "#{config.prefix}:subs_channels",
      "#{config.prefix}:presence:presence-room",
      "#{config.prefix}:presence_channels",
      "#{config.prefix}:node:abc",
      "#{config.prefix}:reaper_lock"
    ]

    assert keys |> Enum.map(&Topology.key_slot/1) |> Enum.uniq() |> length() == 1
  end

  test "without redis_cluster the keys are unchanged" do
    config = Socketelx.Config.resolve(:topology_plain, redis_url: "redis://x")
    assert config.prefix == "socketelx:app-id"
  end

  test "redis_cluster needs a Redis URL" do
    assert_raise ArgumentError, ~r/redis_cluster/, fn ->
      Socketelx.Config.resolve(:topology_nourl, redis_cluster: true)
    end
  end

  test "seed nodes are parsed from a string or a list" do
    config =
      Socketelx.Config.resolve(:topology_seeds,
        redis_url: "redis://x",
        redis_cluster: true,
        redis_cluster_nodes: "a:7000, b:7001,c"
      )

    assert config.redis_cluster_nodes == [{"a", 7000}, {"b", 7001}, {"c", 6379}]
  end
end
