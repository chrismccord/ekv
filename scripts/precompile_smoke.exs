data_dir = Path.join(System.tmp_dir!(), "ekv_precompile_#{System.unique_integer([:positive])}")
{:ok, pid} = EKV.start_link(name: :precompile_smoke, data_dir: data_dir, shards: 1)

try do
  :ok = EKV.put(:precompile_smoke, "smoke", "precompiled")
  "precompiled" = EKV.get(:precompile_smoke, "smoke")
after
  Supervisor.stop(pid)
  File.rm_rf!(data_dir)
end
