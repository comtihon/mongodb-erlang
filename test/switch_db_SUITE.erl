%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. Aug 2018 17:50
%%%-------------------------------------------------------------------
-module(switch_db_SUITE).
-author("tihon").

%% API
-export([]).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("mongo_protocol.hrl").


-compile(export_all).

all() ->
  [
    use_different_dbs_on_one_connection
  ].

init_per_suite(Config) ->
  application:ensure_all_started(mongodb),
  [{database, <<"test">>} | Config].

end_per_suite(_Config) ->
  ok.

init_per_testcase(Case, Config) ->
  {ok, Connection} = mc_worker_api:connect([{database, ?config(database, Config)}, {login, <<"user">>}, {password, <<"test">>}, {w_mode, safe}]),
  [{connection, Connection}, {collection, mc_test_utils:collection(Case)} | Config].

end_per_testcase(_Case, Config) ->
  Connection = ?config(connection, Config),
  Collection = ?config(collection, Config),
  {true, _} = mc_worker_api:delete(Connection, Collection, #{}).

%% Tests
use_different_dbs_on_one_connection(Config) ->
  Connection = ?config(connection, Config),
  Collection = ?config(collection, Config),

  NationalTeams = [
    #{<<"name">> => <<"Mets">>,
      <<"home">> => #{<<"city">> => <<"New York">>, <<"state">> => <<"NY">>},
      <<"league">> => <<"National">>},
    #{<<"name">> => <<"Phillies">>,
      <<"home">> => #{<<"city">> => <<"Philadelphia">>, <<"state">> => <<"PA">>},
      <<"league">> => <<"National">>}
  ],
  AmericanTeams = [
    #{<<"name">> => <<"Yankees">>,
      <<"home">> => #{<<"city">> => <<"New York">>, <<"state">> => <<"NY">>},
      <<"league">> => <<"American">>},
    #{<<"name">> => <<"Red Sox">>,
      <<"home">>=> #{<<"city">> => <<"Boston">>, <<"state">> => <<"MA">>},
      <<"league">> => <<"American">>}
  ],

  {{true, _}, _} = mc_worker_api:insert(Connection, Collection, NationalTeams),
  {ok, NationalTeamsCur} = mc_worker_api:find(Connection, Collection, #{}, #{projector => #{<<"_id">> => false}}),
  NationalTeamsFound = mc_cursor:rest(NationalTeamsCur),
  ?assertEqual(NationalTeams, NationalTeamsFound),

  % switch database and check it
  ok = mc_worker_api:database(Connection, <<"other_test">>),
  [] = mc_worker_api:find(Connection, Collection, #{}, #{projector => #{<<"_id">> => false}}), % no data in another db
  % insert data and check it
  {{true, _}, _} = mc_worker_api:insert(Connection, Collection, AmericanTeams),
  {ok, AmericanTeamsCur} = mc_worker_api:find(Connection, Collection, #{}, #{projector => #{<<"_id">> => false}}),
  AmericanTeamsFound = mc_cursor:rest(AmericanTeamsCur),
  ?assertEqual(AmericanTeams, AmericanTeamsFound),
  % switch back and check
  ok = mc_worker_api:database(Connection, <<"test">>),
  {ok, StillOnlyNationalTeamsCur} = mc_worker_api:find(Connection, Collection, #{}, #{projector => #{<<"_id">> => false}}),
  NationalTeamsFound3 = mc_cursor:rest(StillOnlyNationalTeamsCur),
  ?assertEqual(NationalTeams, NationalTeamsFound3),

  Config.
