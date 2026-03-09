defmodule BenchWebWeb.Router do
  use BenchWebWeb, :router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_root_layout, html: {BenchWebWeb.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
  end

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/", BenchWebWeb do
    pipe_through :browser

    live "/", BenchLive
  end

  # Other scopes may use custom stacks.
  # scope "/api", BenchWebWeb do
  #   pipe_through :api
  # end
end
