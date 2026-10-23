defmodule Socketelx.SizeLimitsTest do
  @moduledoc "Oversized REST bodies and WebSocket frames are refused, normal ones still work."
  use ExUnit.Case, async: false
  @moduletag :capture_log

  alias Socketelx.{Naming, TestWsClient}

  @key "k"
  @secret "s"

  setup do
    name = :"size_#{System.unique_integer([:positive])}"

    start_supervised!(
      Supervisor.child_spec(
        {Socketelx, name: name, port: 0, app_id: "size", app_key: @key, app_secret: @secret},
        id: name
      )
    )

    %{port: :ranch.get_port(Naming.listener(name))}
  end

  test "a huge REST body gets 413 instead of crashing the request", %{port: port} do
    body = Jason.encode!(%{name: "x", channels: ["c"], data: String.duplicate("a", 1_500_000)})
    assert {413, %{"error" => _}} = post(port, "/apps/size/events", body)
  end

  test "an ordinary event is still accepted", %{port: port} do
    body = Jason.encode!(%{name: "x", channels: ["c"], data: String.duplicate("a", 9_000)})
    assert {200, _} = post(port, "/apps/size/events", body)
  end

  test "a huge WebSocket frame closes the connection", %{port: port} do
    {:ok, client} = TestWsClient.start(port, @key)
    assert_receive {:ws, ^client, %{"event" => "pusher:connection_established"}}

    TestWsClient.send_json(client, %{event: "client-big", data: String.duplicate("a", 150_000)})
    assert_receive {:ws_closed, ^client}, 2_000
  end

  test "a normal client message still works", %{port: port} do
    {:ok, client} = TestWsClient.start(port, @key)
    assert_receive {:ws, ^client, %{"event" => "pusher:connection_established"}}

    TestWsClient.send_json(client, %{event: "pusher:ping", data: %{}})
    assert_receive {:ws, ^client, %{"event" => "pusher:pong"}}, 2_000
  end

  defp post(port, path, body) do
    md5 = :crypto.hash(:md5, body) |> Base.encode16(case: :lower)

    params = %{
      "auth_key" => @key,
      "auth_timestamp" => to_string(System.system_time(:second)),
      "auth_version" => "1.0",
      "body_md5" => md5
    }

    query = params |> Enum.sort() |> Enum.map_join("&", fn {k, v} -> "#{k}=#{v}" end)

    signature =
      :crypto.mac(:hmac, :sha256, @secret, "POST\n#{path}\n#{query}")
      |> Base.encode16(case: :lower)

    url = ~c"http://127.0.0.1:#{port}#{path}?#{query}&auth_signature=#{signature}"

    {:ok, {{_, status, _}, _h, resp}} =
      :httpc.request(:post, {url, [], ~c"application/json", body}, [], [])

    {status, if(resp == [], do: %{}, else: Jason.decode!(to_string(resp)))}
  end
end
