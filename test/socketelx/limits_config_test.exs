defmodule Socketelx.LimitsConfigTest do
  # Reads and writes the OS environment, so it must not run alongside other tests.
  use ExUnit.Case, async: false
  @moduletag :capture_log

  alias Socketelx.{Config, RateLimiter}

  @vars ~w(REST_RATE_LIMIT CLIENT_EVENT_RATE_LIMIT MAX_CONNECTIONS MAX_CONNECTIONS_PER_IP)
  @env Enum.flat_map(@vars, &["SOCKETELX_" <> &1, "PUSHER_" <> &1])
  @keys [:rest_rate_limit, :client_event_rate_limit, :max_connections, :max_connections_per_ip]

  setup do
    saved = Map.new(@env, &{&1, System.get_env(&1)})
    Enum.each(@env, &System.delete_env/1)

    on_exit(fn ->
      Enum.each(saved, fn
        {k, nil} -> System.delete_env(k)
        {k, v} -> System.put_env(k, v)
      end)
    end)
  end

  test "limits are off when unset" do
    config = Config.resolve(:limits_unset, [])
    assert Enum.all?(@keys, &(Map.fetch!(config, &1) == nil))
  end

  test "0 in the options means no limit" do
    config = Config.resolve(:limits_zero_opts, Enum.map(@keys, &{&1, 0}))
    assert Enum.all?(@keys, &(Map.fetch!(config, &1) == nil))
  end

  test "0 in the environment means no limit" do
    Enum.each(@vars, &System.put_env("SOCKETELX_" <> &1, "0"))
    config = Config.resolve(:limits_zero_env, [])
    assert Enum.all?(@keys, &(Map.fetch!(config, &1) == nil))
  end

  test "negative numbers also mean no limit" do
    config = Config.resolve(:limits_negative, rest_rate_limit: -5, max_connections: -1)
    assert config.rest_rate_limit == nil
    assert config.max_connections == nil
  end

  test "positive numbers still apply" do
    config = Config.resolve(:limits_positive, Enum.map(@keys, &{&1, 7}))
    assert Enum.all?(@keys, &(Map.fetch!(config, &1) == 7))
  end

  test "a limit of nil lets every request through" do
    assert RateLimiter.allow?(:limits_nil, :rest, nil)
  end
end
