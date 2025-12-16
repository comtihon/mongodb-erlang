-module(tuple_normalization_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% CT callbacks
-export([all/0, init_per_suite/1, end_per_suite/1]).

%% Test cases
-export([
  test_empty_tuple_to_map/1,
  test_single_kv_tuple_to_map/1,
  test_nested_tuples_to_maps/1,
  test_map_with_tuple_values/1,
  test_proplist_to_map/1,
  test_mixed_nested_structures/1,
  test_already_normalized_map/1,
  test_atom_keys_to_binary/1,
  test_regular_list_unchanged/1,
  test_deep_nesting/1
]).

all() ->
  [
    test_empty_tuple_to_map,
    test_single_kv_tuple_to_map,
    test_nested_tuples_to_maps,
    test_map_with_tuple_values,
    test_proplist_to_map,
    test_mixed_nested_structures,
    test_already_normalized_map,
    test_atom_keys_to_binary,
    test_regular_list_unchanged,
    test_deep_nesting
  ].

init_per_suite(Config) ->
  Config.

end_per_suite(_Config) ->
  ok.

%% Helper to access private functions
normalize_map_values(Map) ->
  mc_connection_man:normalize_map_values(Map).

%% Test Cases

test_empty_tuple_to_map(_Config) ->
  %% Empty tuple should become empty map
  Input = #{<<"sort">> => {}},
  Expected = #{<<"sort">> => #{}},
  Result = normalize_map_values(Input),
  ?assertEqual(Expected, Result).

test_single_kv_tuple_to_map(_Config) ->
  %% Single key-value tuple should become a map
  Input = #{<<"query">> => {logical_name, <<"test123">>}},
  Expected = #{<<"query">> => #{<<"logical_name">> => <<"test123">>}},
  Result = normalize_map_values(Input),
  ?assertEqual(Expected, Result).

test_nested_tuples_to_maps(_Config) ->
  %% Nested tuples should be recursively converted
  Input = #{
    <<"update">> => #{
      <<"$set">> => {os_version, <<"v1.0">>}
    }
  },
  Expected = #{
    <<"update">> => #{
      <<"$set">> => #{<<"os_version">> => <<"v1.0">>}
    }
  },
  Result = normalize_map_values(Input),
  ?assertEqual(Expected, Result).

test_map_with_tuple_values(_Config) ->
  %% Map with multiple tuple values
  Input = #{
    <<"query">> => {device_id, <<"abc123">>},
    <<"sort">> => {},
    <<"update">> => {status, <<"active">>}
  },
  Expected = #{
    <<"query">> => #{<<"device_id">> => <<"abc123">>},
    <<"sort">> => #{},
    <<"update">> => #{<<"status">> => <<"active">>}
  },
  Result = normalize_map_values(Input),
  ?assertEqual(Expected, Result).

test_proplist_to_map(_Config) ->
  %% Proplist should be converted to map
  Input = #{
    <<"fields">> => [{name, <<"test">>}, {age, 25}]
  },
  Expected = #{
    <<"fields">> => #{<<"name">> => <<"test">>, <<"age">> => 25}
  },
  Result = normalize_map_values(Input),
  ?assertEqual(Expected, Result).

test_mixed_nested_structures(_Config) ->
  %% Complex nested structure with tuples, maps, and lists
  Input = #{
    <<"findAndModify">> => <<"devices">>,
    <<"query">> => {logical_name, <<"uuid-123">>},
    <<"sort">> => {},
    <<"update">> => #{
      <<"$set">> => {os_version, <<"2.0">>}
    },
    <<"new">> => true
  },
  Expected = #{
    <<"findAndModify">> => <<"devices">>,
    <<"query">> => #{<<"logical_name">> => <<"uuid-123">>},
    <<"sort">> => #{},
    <<"update">> => #{
      <<"$set">> => #{<<"os_version">> => <<"2.0">>}
    },
    <<"new">> => true
  },
  Result = normalize_map_values(Input),
  ?assertEqual(Expected, Result).

test_already_normalized_map(_Config) ->
  %% Already normalized map should pass through unchanged
  Input = #{
    <<"query">> => #{<<"logical_name">> => <<"test">>},
    <<"sort">> => #{},
    <<"limit">> => 10
  },
  Expected = Input,
  Result = normalize_map_values(Input),
  ?assertEqual(Expected, Result).

test_atom_keys_to_binary(_Config) ->
  %% Atom keys should be converted to binary
  Input = #{
    <<"query">> => {device_id, <<"123">>}
  },
  Expected = #{
    <<"query">> => #{<<"device_id">> => <<"123">>}
  },
  Result = normalize_map_values(Input),
  ?assertEqual(Expected, Result).

test_regular_list_unchanged(_Config) ->
  %% Regular lists (not proplists) should remain unchanged
  Input = #{
    <<"tags">> => [<<"tag1">>, <<"tag2">>, <<"tag3">>]
  },
  Expected = Input,
  Result = normalize_map_values(Input),
  ?assertEqual(Expected, Result).

test_deep_nesting(_Config) ->
  %% Deeply nested structures should be fully normalized
  Input = #{
    <<"level1">> => #{
      <<"level2">> => #{
        <<"level3">> => {key, <<"value">>}
      }
    }
  },
  Expected = #{
    <<"level1">> => #{
      <<"level2">> => #{
        <<"level3">> => #{<<"key">> => <<"value">>}
      }
    }
  },
  Result = normalize_map_values(Input),
  ?assertEqual(Expected, Result).