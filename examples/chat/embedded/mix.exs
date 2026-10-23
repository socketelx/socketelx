defmodule ChatEmbedded.MixProject do
  use Mix.Project

  def project do
    [
      app: :chat_embedded,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger], mod: {Chat.Application, []}]
  end

  defp deps do
    [
      # trigger/5 and authorize_channel/4 need socketelx 0.2 or newer
      {:socketelx, "~> 0.5"},
      {:plug_cowboy, "~> 2.7"},
      {:jason, "~> 1.4"}
    ]
  end
end
