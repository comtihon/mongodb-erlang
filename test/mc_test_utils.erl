%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. Dec 2016 20:42
%%%-------------------------------------------------------------------
-module(mc_test_utils).
-author("tihon").

%% API
-export([collection/1]).

collection(Case) ->
  Now = now_to_seconds(os:timestamp()),
  <<(atom_to_binary(?MODULE, utf8))/binary, $-,
    (atom_to_binary(Case, utf8))/binary, $-,
    (list_to_binary(integer_to_list(Now)))/binary>>.

%% @private
now_to_seconds({Mega, Sec, _}) ->
  (Mega * 1000000) + Sec.