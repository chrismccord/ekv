defmodule EKV.Event do
  @moduledoc """
  Represents a key change event delivered to subscribers.

  - `:put` — `value` is the new value being written
  - `:delete` — `value` is the previous value before deletion (or `nil` if the key didn't exist locally)
  - `:expired` — `value` is the last local value observed before TTL expiry
  """

  defstruct [:type, :key, :value]

  @type t :: %__MODULE__{type: :put | :delete | :expired, key: String.t(), value: term() | nil}
end
