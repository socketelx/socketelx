defmodule Socketelx.MixProject do
  use Mix.Project

  @source_url "https://github.com/socketelx/socketelx"
  @version "0.5.1"

  def project do
    [
      app: :socketelx,
      version: @version,
      elixir: "~> 1.16",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      releases: [
        socketelx: [
          include_executables_for: [:unix]
        ]
      ],

      # --- Hex package metadata (fill in before `mix hex.publish`) ---
      description:
        "A Pusher-compatible WebSocket server for Elixir, inspired by Laravel Reverb. " <>
          "Run it as a Docker image or embed it in your Elixir app.",
      package: package(),
      docs: docs(),
      source_url: @source_url,
      homepage_url: @source_url
    ]
  end

  def application do
    [
      extra_applications: [:logger, :crypto, :inets, :ssl],
      mod: {Socketelx.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:plug, "~> 1.16"},
      {:plug_cowboy, "~> 2.7"},
      {:jason, "~> 1.4"},
      {:redix, "~> 1.5"},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => @source_url <> "/blob/main/CHANGELOG.md"
      },
      files: ~w(lib guides mix.exs README.md LICENSE CHANGELOG.md)
    ]
  end

  defp docs do
    [
      main: "readme",
      source_ref: "v#{@version}",
      extras: [
        "README.md",
        "guides/getting-started.md",
        "guides/configuration.md",
        "guides/embedding.md",
        "guides/multi-app.md",
        "guides/multi-server.md",
        "guides/deployment.md",
        "guides/production.md",
        "guides/capacity-planning.md",
        "guides/protocol-support.md",
        "guides/load-testing.md",
        "guides/troubleshooting.md",
        "guides/laravel.md",
        {"examples/chat/README.md", [filename: "chat-example", title: "Chat example"]},
        "CONTRIBUTING.md",
        "SECURITY.md",
        "CHANGELOG.md"
      ],
      groups_for_extras: [
        Guides: ~r{guides/},
        Examples: ["examples/chat/README.md"],
        Project: ["CONTRIBUTING.md", "SECURITY.md", "CHANGELOG.md"]
      ],
      groups_for_modules: [
        "Public API": [Socketelx],
        Configuration: [Socketelx.Config],
        Backends: [
          Socketelx.Backend,
          Socketelx.Backend.Local,
          Socketelx.Backend.Redis,
          Socketelx.Backend.Distributed
        ],
        Internals:
          ~r/^Socketelx\.(?!Backend$|Backend\.Local$|Backend\.Redis$|Backend\.Distributed$|Config$)/
      ]
    ]
  end
end
