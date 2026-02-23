# Start distribution if not already running (needed for distributed tests)
unless Node.alive?() do
  {:ok, _} = Node.start(:"test_#{System.unique_integer([:positive])}@127.0.0.1", :longnames)
  Node.set_cookie(:ekv_test)
end

:application.set_env(:kernel, :prevent_overlapping_partitions, false)

ExUnit.start()
