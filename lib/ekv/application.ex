defmodule EKV.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      %{id: :ekv_pg, start: {:pg, :start_link, []}}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: EKV.AppSupervisor)
  end
end
