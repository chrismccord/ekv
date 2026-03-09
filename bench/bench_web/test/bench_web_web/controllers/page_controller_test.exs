defmodule BenchWebWeb.PageControllerTest do
  use BenchWebWeb.ConnCase

  test "GET /", %{conn: conn} do
    conn = get(conn, ~p"/")
    body = html_response(conn, 200)
    assert body =~ "EKV CAS Bench"
    assert body =~ "Fly Orchestrator"
  end
end
