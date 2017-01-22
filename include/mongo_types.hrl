%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 17. Dec 2016 22:21
%%%-------------------------------------------------------------------
-author("tihon").

-ifndef(MONGO_TYPES).
-define(MONGO_TYPES, true).

-include("mongo_protocol.hrl").

-type cursorid() :: integer().
-type selector() :: map() | bson:document().
-type projector() :: bson:document() | map().
-type skip() :: integer().
-type batchsize() :: integer(). % 0 = default batch size. negative closes cursor
-type modifier() :: bson:document() | map().
-type connection() :: pid().
-type args() :: [arg()].
-type arg() :: {database, database()}
| {login, binary()}
| {password, binary()}
| {w_mode, write_mode()}
| {r_mode, read_mode()}
| {host, list()}
| {port, integer()}
| {ssl, boolean()}
| {ssl_opts, proplists:proplist()}
| {register, atom() | fun()}.
-type write_mode() :: unsafe | safe | {safe, bson:document()}.
-type read_mode() :: master | slave_ok.
-type service() :: {Host :: inet:hostname() | inet:ip_address(), Post :: 0..65535}.
-type options() :: [option()].
-type option() :: {timeout, timeout()} | {ssl, boolean()} | ssl | {database, database()} | {read_mode, read_mode()} | {write_mode, write_mode()}.
-type cursor() :: pid().
-type query() :: #query{}.
-endif.