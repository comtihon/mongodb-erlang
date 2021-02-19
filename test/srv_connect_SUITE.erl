%%%-------------------------------------------------------------------
%%% @author Drew Varner
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%  This suite tests connecting via MongoDB DNS SRV records. This
%%%  test will be skipped unless the following environment variables
%%%  are set: ERL_MONGODB_CT_SRV, ERL_MONGODB_CT_SRV_LOGIN,
%%%  ERL_MONGODB_CT_SRV_PASSWORD and ERL_MONGODB_CT_SRV_DB.
%%%  The environment variable should point to a MongoDB instance
%%%  reachable by DNS SRV records. I used the free tier of Mongo's Atlas
%%%  Cloud database offering in my testing.
%%% @end
%%%-------------------------------------------------------------------
-module(srv_connect_SUITE).

%% API
-export([]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [fetch_srv_seeds_test, invalid_srv_test, connect_via_srv_test].

init_per_suite(Config) ->
    application:ensure_all_started(mongodb),
    VarNames = [{srv, "ERL_MONGODB_CT_SRV"},
                {login, "ERL_MONGODB_CT_SRV_LOGIN"},
                {password, "ERL_MONGODB_CT_SRV_PASSWORD"},
                {database, "ERL_MONGODB_CT_SRV_DB"}],
    Vars = [{Key, os:getenv(Var, "")} || {Key, Var} <- VarNames],
    case lists:any(fun ({_Key, Val}) ->
                           Val == ""
                   end,
                   Vars)
        of
      true ->
          {skipped, no_srv_env_vars};
      false ->
          Vars ++ Config
    end.

end_per_suite(_Config) ->
    ok.

%% Tests
fetch_srv_seeds_test(Config) ->
    {ok, Seeds} = mc_utils:get_srv_seeds(?config(srv, Config)),
    ?assert(length(Seeds) > 1).

invalid_srv_test(_Config) ->
    Rslt = mc_utils:get_srv_seeds("thisisnotevenarealdomain.ai"),
    ?assertEqual({error, srv_lookup_failed}, Rslt).

connect_via_srv_test(Config) ->
    {ok, Connection} = mc_worker_api:connect([{srv, bin_config(srv, Config)},
                                              {login, bin_config(login, Config)},
                                              {password, bin_config(password, Config)},
                                              {database, bin_config(database, Config)},
                                              {ssl, true}]),
    ok = mc_worker_api:disconnect(Connection).

bin_config(Key, Config) ->
    list_to_binary(?config(Key, Config)).
