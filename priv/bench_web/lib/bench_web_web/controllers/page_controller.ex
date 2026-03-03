defmodule BenchWebWeb.PageController do
  use BenchWebWeb, :controller

  def home(conn, _params) do
    render(conn, :home)
  end
end
