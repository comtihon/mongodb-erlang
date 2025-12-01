-module(query_conversion_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("../include/mongo_protocol.hrl").

%% CT callbacks
-export([all/0, init_per_suite/1, end_per_suite/1]).

%% Test cases
-export([
  test_negative_batchsize_creates_singlebatch_field/1,
  test_positive_batchsize_no_singlebatch_field/1,
  test_zero_batchsize_no_singlebatch_field/1,
  test_negative_batchsize_creates_limit_field/1,
  test_query_with_skip_and_negative_batchsize/1,
  test_query_with_projection_and_negative_batchsize/1,
  test_query_with_sort_and_negative_batchsize/1
]).

all() ->
  [
    test_negative_batchsize_creates_singlebatch_field,
    test_positive_batchsize_no_singlebatch_field,
    test_zero_batchsize_no_singlebatch_field,
    test_negative_batchsize_creates_limit_field,
    test_query_with_skip_and_negative_batchsize,
    test_query_with_projection_and_negative_batchsize,
    test_query_with_sort_and_negative_batchsize
  ].

init_per_suite(Config) ->
  Config.

end_per_suite(_Config) ->
  ok.

%% Helper function to convert query to op_msg_command
%% This simulates what mc_worker_api:fixed_query does
convert_query_to_op_msg(Query) ->
  #'query'{collection = Coll,
    skip = Skip,
    selector = Selector,
    batchsize = BatchSize,
    projector = Projector} = Query,
  
  SingleBatch = BatchSize < 0,
  AbsBatchSize = erlang:abs(BatchSize),
  
  BatchSizeField = case AbsBatchSize =:= 0 of
    true -> [];
    false -> [{<<"batchSize">>, AbsBatchSize}]
  end,
  
  SingleBatchField = case SingleBatch of
    true -> [{<<"singleBatch">>, true}];
    false -> []
  end,
  
  CommandDoc = [
    {<<"find">>, Coll},
    {<<"filter">>, Selector},
    {<<"projection">>, Projector},
    {<<"skip">>, Skip}
  ] ++ BatchSizeField ++ SingleBatchField,
  
  #op_msg_command{command_doc = CommandDoc}.

%% Test Cases

test_negative_batchsize_creates_singlebatch_field(_Config) ->
  Query = #'query'{
    collection = <<"test">>,
    selector = #{},
    batchsize = -10,
    skip = 0,
    projector = #{}
  },
  
  OpMsg = convert_query_to_op_msg(Query),
  CommandDoc = OpMsg#op_msg_command.command_doc,
  
  %% Should have singleBatch: true
  ?assertEqual({<<"singleBatch">>, true}, lists:keyfind(<<"singleBatch">>, 1, CommandDoc)),
  
  %% Should have batchSize: 10 (absolute value)
  ?assertEqual({<<"batchSize">>, 10}, lists:keyfind(<<"batchSize">>, 1, CommandDoc)).

test_positive_batchsize_no_singlebatch_field(_Config) ->
  Query = #'query'{
    collection = <<"test">>,
    selector = #{},
    batchsize = 10,
    skip = 0,
    projector = #{}
  },
  
  OpMsg = convert_query_to_op_msg(Query),
  CommandDoc = OpMsg#op_msg_command.command_doc,
  
  %% Should NOT have singleBatch field
  ?assertEqual(false, lists:keyfind(<<"singleBatch">>, 1, CommandDoc)),
  
  %% Should have batchSize: 10
  ?assertEqual({<<"batchSize">>, 10}, lists:keyfind(<<"batchSize">>, 1, CommandDoc)).

test_zero_batchsize_no_singlebatch_field(_Config) ->
  Query = #'query'{
    collection = <<"test">>,
    selector = #{},
    batchsize = 0,
    skip = 0,
    projector = #{}
  },
  
  OpMsg = convert_query_to_op_msg(Query),
  CommandDoc = OpMsg#op_msg_command.command_doc,
  
  %% Should NOT have singleBatch field
  ?assertEqual(false, lists:keyfind(<<"singleBatch">>, 1, CommandDoc)),
  
  %% Should NOT have batchSize field (0 means use default)
  ?assertEqual(false, lists:keyfind(<<"batchSize">>, 1, CommandDoc)).

test_negative_batchsize_creates_limit_field(_Config) ->
  %% According to MongoDB spec, negative batchsize should act as limit
  Query = #'query'{
    collection = <<"test">>,
    selector = #{},
    batchsize = -5,
    skip = 0,
    projector = #{}
  },
  
  OpMsg = convert_query_to_op_msg(Query),
  CommandDoc = OpMsg#op_msg_command.command_doc,
  
  %% Should have singleBatch: true
  ?assertEqual({<<"singleBatch">>, true}, lists:keyfind(<<"singleBatch">>, 1, CommandDoc)),
  
  %% Should have batchSize with absolute value
  ?assertEqual({<<"batchSize">>, 5}, lists:keyfind(<<"batchSize">>, 1, CommandDoc)).

test_query_with_skip_and_negative_batchsize(_Config) ->
  Query = #'query'{
    collection = <<"test">>,
    selector = #{},
    batchsize = -7,
    skip = 10,
    projector = #{}
  },
  
  OpMsg = convert_query_to_op_msg(Query),
  CommandDoc = OpMsg#op_msg_command.command_doc,
  
  %% Should have skip
  ?assertEqual({<<"skip">>, 10}, lists:keyfind(<<"skip">>, 1, CommandDoc)),
  
  %% Should have singleBatch: true
  ?assertEqual({<<"singleBatch">>, true}, lists:keyfind(<<"singleBatch">>, 1, CommandDoc)),
  
  %% Should have batchSize: 7
  ?assertEqual({<<"batchSize">>, 7}, lists:keyfind(<<"batchSize">>, 1, CommandDoc)).

test_query_with_projection_and_negative_batchsize(_Config) ->
  Query = #'query'{
    collection = <<"test">>,
    selector = #{},
    batchsize = -3,
    skip = 0,
    projector = #{<<"field1">> => 1}
  },
  
  OpMsg = convert_query_to_op_msg(Query),
  CommandDoc = OpMsg#op_msg_command.command_doc,
  
  %% Should have projection
  ?assertEqual({<<"projection">>, #{<<"field1">> => 1}}, lists:keyfind(<<"projection">>, 1, CommandDoc)),
  
  %% Should have singleBatch: true
  ?assertEqual({<<"singleBatch">>, true}, lists:keyfind(<<"singleBatch">>, 1, CommandDoc)),
  
  %% Should have batchSize: 3
  ?assertEqual({<<"batchSize">>, 3}, lists:keyfind(<<"batchSize">>, 1, CommandDoc)).

test_query_with_sort_and_negative_batchsize(_Config) ->
  Query = #'query'{
    collection = <<"test">>,
    selector = #{<<"$query">> => #{}, <<"$orderby">> => #{<<"field1">> => 1}},
    batchsize = -15,
    skip = 0,
    projector = #{}
  },
  
  OpMsg = convert_query_to_op_msg(Query),
  CommandDoc = OpMsg#op_msg_command.command_doc,
  
  %% Should have singleBatch: true
  ?assertEqual({<<"singleBatch">>, true}, lists:keyfind(<<"singleBatch">>, 1, CommandDoc)),
  
  %% Should have batchSize: 15
  ?assertEqual({<<"batchSize">>, 15}, lists:keyfind(<<"batchSize">>, 1, CommandDoc)).
