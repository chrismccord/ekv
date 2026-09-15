defmodule EKV.LocalModelPropertyTest do
  use ExUnit.Case, async: false
  use ExUnitProperties

  alias EKV.PropertyHelpers, as: H

  @moduletag :tmp_dir
  @moduletag :capture_log
  @keys ["model/a", "model/b", "model/c"]

  property "local LWW puts, deletes and reopens match a map", %{tmp_dir: root} do
    check all(
            commands <- list_of(lww_command(), max_length: 25),
            value <- integer(-2..2)
          ) do
      # Retained during shrinking: put, delete, recreate and durable reopen.
      commands =
        [{:put, "model/a", value}, {:delete, "model/a"}, :reopen, {:put, "model/a", value}] ++
          commands ++ [:reopen]

      H.in_directory(root, fn directory ->
        H.sessions(
          commands,
          %{},
          fn -> H.open_member(directory, :lww_property) end,
          &H.close_member/1,
          &lww_step/3,
          fn {name, _pid}, model -> assert_values(name, model) end
        )
      end)
    end
  end

  property "single-member CAS uses symbolic versions across deletes and reopens", %{tmp_dir: root} do
    check all(
            blocks <- list_of(tuple({integer(-2..2), cas_command()}), max_length: 12),
            first <- integer(-2..2),
            second <- integer(-2..2)
          ) do
      # :current and {:prior, distance} resolve against successful writes at
      # execution time, not timestamps baked into generated commands.
      # Pair each probe with a successful write so rejected calls cannot
      # dominate a generated history (even after shrinking).
      commands =
        Enum.flat_map(blocks, fn {value, probe} -> [{:put, value, :current}, probe] end)

      commands =
        [
          {:put, first, :absent},
          {:put, second, :current},
          {:put, first, {:prior, 1}},
          {:delete, :current},
          :reopen,
          {:put, first, :absent},
          :eventual_put,
          :eventual_delete
        ] ++ commands ++ [:reopen]

      H.in_directory(root, fn directory ->
        model =
          H.sessions(
            commands,
            %{value: nil, current: nil, versions: [], applied: 0, conflicts: 0, rejected: 0},
            fn -> H.open_member(directory, :cas_property) end,
            &H.close_member/1,
            &cas_step/3,
            &assert_cas/2
          )

        # Actual outcomes, not just generated operation names.
        assert model.applied >= 4
        assert model.conflicts >= 1
        assert model.rejected >= 2
        assert model.applied >= model.conflicts + model.rejected
      end)
    end
  end

  defp lww_command do
    one_of([
      tuple({constant(:put), member_of(@keys), integer(-2..2)}),
      tuple({constant(:delete), member_of(@keys)}),
      constant(:reopen)
    ])
  end

  defp cas_command do
    reference =
      one_of([constant(:current), constant(:absent), tuple({constant(:prior), integer(0..3)})])

    one_of([
      tuple({constant(:put), integer(-2..2), reference}),
      tuple({constant(:delete), reference}),
      constant(:eventual_put),
      constant(:eventual_delete),
      constant(:reopen)
    ])
  end

  defp lww_step({name, _pid}, {:put, key, value}, model) do
    assert :ok = EKV.put(name, key, value)
    Map.put(model, key, value)
  end

  defp lww_step({name, _pid}, {:delete, key}, model) do
    assert :ok = EKV.delete(name, key)
    Map.delete(model, key)
  end

  defp cas_step({name, _pid}, :eventual_put, model) do
    assert {:error, :cas_managed_key} = EKV.put(name, "model/a", :not_applied)
    %{model | rejected: model.rejected + 1}
  end

  defp cas_step({name, _pid}, :eventual_delete, model) do
    assert {:error, :cas_managed_key} = EKV.delete(name, "model/a")
    %{model | rejected: model.rejected + 1}
  end

  defp cas_step({name, _pid}, command, model) do
    {value, reference} =
      case command do
        {:put, value, reference} -> {value, reference}
        {:delete, reference} -> {nil, reference}
      end

    expected_id =
      case reference do
        :absent -> nil
        :current -> model.current
        {:prior, distance} -> max(length(model.versions) - 1 - distance, 0)
      end

    expected_vsn = if expected_id != nil, do: Enum.fetch!(model.versions, expected_id)

    result =
      case command do
        {:put, _, _} -> EKV.put(name, "model/a", value, if_vsn: expected_vsn)
        {:delete, _} -> EKV.delete(name, "model/a", if_vsn: expected_vsn)
      end

    # The oracle compares symbolic write identities; opaque implementation
    # versions are only used to supply API inputs and check returned versions.
    if expected_id == model.current do
      assert {:ok, {timestamp, "property-member"} = vsn} = result
      assert is_integer(timestamp)
      assert vsn not in model.versions

      %{
        model
        | value: value,
          current: if(value != nil, do: length(model.versions)),
          versions: model.versions ++ [vsn],
          applied: model.applied + 1
      }
    else
      assert result == {:error, :conflict}
      %{model | conflicts: model.conflicts + 1}
    end
  end

  defp assert_cas({name, _pid}, model) do
    expected = if model.current == nil, do: %{}, else: %{"model/a" => model.value}
    assert_values(name, expected)

    expected_lookup =
      if model.current != nil,
        do: {model.value, Enum.fetch!(model.versions, model.current)}

    assert EKV.lookup(name, "model/a") == expected_lookup
    assert EKV.get(name, "model/a", consistent: true) == model.value
  end

  defp assert_values(name, model) do
    # Local completed writes are immediately visible; this is NOT an
    # assertion about eventual reads on another member.
    for key <- @keys do
      assert EKV.get(name, key) == Map.get(model, key)
    end

    rows = Enum.to_list(EKV.scan(name, "model/"))
    assert Map.new(rows, fn {key, value, _vsn} -> {key, value} end) == model
    assert length(rows) == map_size(model)

    assert Enum.map(EKV.keys(name, "model/"), &elem(&1, 0)) |> Enum.sort() ==
             Enum.sort(Map.keys(model))
  end
end
