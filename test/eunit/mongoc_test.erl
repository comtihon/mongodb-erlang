%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 17. Dec 2016 21:56
%%%-------------------------------------------------------------------
-module(mongoc_test).
-author("tihon").

-include("mongo_protocol.hrl").
-include_lib("eunit/include/eunit.hrl").

deprecated_find_transformation_test() ->
  Query = mongoc:find_query(
    #{server_type => mongos, read_preference => #{mode => secondaryPreferred, tags => []}},
    <<"test">>, {<<"name">>, <<"test">>}, #{<<"_id">> => true}, 17, 9),

  ExpectedQuery = #query{
    collection = <<"test">>,
    selector = #{
      <<"$query">> => {<<"name">>, <<"test">>},
      <<"$readPreference">> => #{mode => <<"secondaryPreferred">>}
    },
    projector = #{<<"_id">> => true},
    skip = 17,
    batchsize = 9,
    slaveok = true,
    sok_overriden = true
  },

  ?assertEqual(ExpectedQuery, Query).

deprecated_count_transformation_test() ->
  Query = mongoc:count_query(
    #{server_type => mongos, read_preference => #{mode => secondaryPreferred, tags => []}},
    <<"test">>, {<<"name">>, <<"test">>}, 99),

  ExpectedQuery = #query{
    collection = <<"$cmd">>,
    selector =
    {<<"count">>, <<"test">>,
      <<"query">>, {<<"name">>, <<"test">>},
      <<"limit">>, 99,
      <<"$readPreference">>, #{mode => <<"secondaryPreferred">>}
    },
    projector = #{},
    slaveok = true,
    sok_overriden = true
  },

  ?assertEqual(ExpectedQuery, Query).
