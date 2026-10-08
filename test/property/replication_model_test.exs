defmodule EKV.ReplicationModelPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias EKV.PropertyHelpers, as: H
  alias EKV.Store

  @moduletag :tmp_dir
  @origins ["a", "b"]
  @keys ["model/a", "model/b", "model/c"]

  property "LWW delivery, duplicates, batches and repair preserve winners and contiguous progress",
           %{
             tmp_dir: root
           } do
    check all(
            a <- list_of(row_spec(), max_length: 6),
            b <- list_of(row_spec(), max_length: 6),
            deliveries <- list_of(delivery(), max_length: 20),
            batch_size <- integer(1..3)
          ) do
      streams = %{"a" => stream(a), "b" => stream(b)}

      # Guarantee a gap, a losing row, a duplicate, a tie between origins,
      # single-entry and multi-entry batches, and restart even after shrinking.
      commands =
        [
          {:single, "a", [1]},
          :reopen,
          {:single, "a", [0]},
          {:single, "a", [1]},
          {:batch, "b", [0]},
          {:batch, "b", [0, 1]}
        ] ++ deliveries ++ repair(streams, batch_size) ++ [:reopen]

      H.in_directory(root, fn directory ->
        initial = %{rows: %{}, seen: %{}, applied: 0, ignored: 0, gaps: 0}
        step = fn resource, command, model -> deliver(resource, command, model, streams) end

        model =
          H.sessions(
            commands,
            initial,
            fn -> H.open_store(Path.join(directory, "left")) end,
            &H.close_store/1,
            step,
            &assert_store/2
          )

        assert model.applied > 0
        assert model.ignored >= 2
        assert model.gaps > 0

        # A second real SQLite store receives the complete history in a
        # different order and grouping. Repair is explicit, not a scheduler.
        other =
          H.sessions(
            Enum.reverse(repair(streams, 1)) ++ [:reopen],
            initial,
            fn -> H.open_store(Path.join(directory, "right")) end,
            &H.close_store/1,
            step,
            &assert_store/2
          )

        assert model.rows == other.rows
        assert model.seen == other.seen

        for origin <- @origins do
          assert contiguous(Map.fetch!(model.seen, origin)) == length(streams[origin])
        end
      end)
    end
  end

  defp row_spec do
    tuple({member_of(@keys), integer(-2..2), boolean(), integer(0..2)})
  end

  defp delivery do
    one_of([
      constant(:reopen),
      tuple({constant(:single), member_of(@origins), list_of(integer(0..7), length: 1)}),
      tuple(
        {constant(:batch), member_of(@origins),
         list_of(integer(0..7), min_length: 1, max_length: 3)}
      )
    ])
  end

  defp stream(specs) do
    # Unique, increasing timestamps within an origin; collisions across
    # origins are intentional. Never invent different payloads for the same
    # (origin, sequence), which would be outside the non-malicious model.
    specs = [{"model/a", 0, false, 0}, {"model/a", 1, true, 0}] ++ specs

    specs
    |> Enum.with_index(1)
    |> Enum.map(fn {{key, value, delete?, offset}, seq} ->
      timestamp = seq * 4 + offset
      {key, :erlang.term_to_binary(value), timestamp, seq, nil, if(delete?, do: timestamp)}
    end)
  end

  defp repair(streams, batch_size) do
    for origin <- @origins,
        indices <- Enum.chunk_every(Enum.to_list(0..(length(streams[origin]) - 1)), batch_size) do
      {:batch, origin, indices}
    end
  end

  defp deliver({db, stmts}, {kind, origin, indices}, model, streams) do
    entries = Enum.map(indices, &Enum.at(streams[origin], rem(&1, length(streams[origin]))))

    {expected_flags, next} =
      Enum.map_reduce(entries, model, fn {key, value, ts, seq, expires, deleted}, model ->
        row = {value, ts, origin, expires, deleted}
        old = Map.get(model.rows, key)
        wins? = old == nil or {ts, origin} > {elem(old, 1), elem(old, 2)}
        seen = Map.update(model.seen, origin, MapSet.new([seq]), &MapSet.put(&1, seq))

        next = %{
          model
          | rows: if(wins?, do: Map.put(model.rows, key, row), else: model.rows),
            seen: seen,
            applied: model.applied + if(wins?, do: 1, else: 0),
            ignored: model.ignored + if(wins?, do: 0, else: 1),
            gaps:
              model.gaps + if(contiguous(seen[origin]) < Enum.max(seen[origin]), do: 1, else: 0)
        }

        {wins?, next}
      end)

    {flags, cursor} =
      case {kind, entries} do
        {:single, [{key, value, ts, seq, expires, deleted}]} ->
          assert {:ok, applied, ^seq, progress} =
                   Store.write_entry(
                     db,
                     stmts.kv_upsert,
                     stmts.keyref_upsert,
                     stmts.oplog_insert,
                     key,
                     value,
                     ts,
                     origin,
                     expires,
                     deleted,
                     seq
                   )

          {[applied], progress}

        {:batch, entries} ->
          assert {:ok, flags, _last_seq, progress} =
                   Store.write_entries_batch(
                     db,
                     stmts.kv_upsert,
                     stmts.keyref_upsert,
                     stmts.oplog_insert,
                     origin,
                     entries
                   )

          {flags, progress}
      end

    assert flags == expected_flags
    assert cursor == contiguous(next.seen[origin])
    next
  end

  defp assert_store({db, _stmts}, model) do
    for key <- @keys, do: assert(Store.get(db, key) == Map.get(model.rows, key))
    progress = Store.local_progress_summary(db)

    for origin <- @origins do
      seen = Map.get(model.seen, origin, MapSet.new())
      assert Map.get(progress, origin, 0) == contiguous(seen)

      # Losing LWW rows still belong to the replay origin stream.
      {replay, false} = Store.replay_since_origin_chunk(db, origin, 0, 100, 1_000_000)
      assert Enum.map(replay, &elem(&1, 4)) == Enum.sort(seen)
      assert Enum.all?(replay, &(elem(&1, 3) == origin))
    end
  end

  defp contiguous(seen), do: advance(seen, 0)

  defp advance(seen, cursor) do
    if MapSet.member?(seen, cursor + 1), do: advance(seen, cursor + 1), else: cursor
  end
end
