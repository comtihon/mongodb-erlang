%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2014, <COMPANY>
%%% @doc  mongo client connection manager
%%% processes request and response to|from database
%%% @end
%%% Created : 28. май 2014 18:37
%%%-------------------------------------------------------------------
-module(mc_connection_man).
-author("tihon").

-include("mongo_types.hrl").
-include("mongo_protocol.hrl").

-define(NOT_MASTER_ERROR, 13435).
-define(UNAUTHORIZED_ERROR(C), C =:= 10057; C =:= 16550).

%% API
-export([request_worker/2, process_reply/2]).
-export([read/2, read_one/2, read_one_sync/4]).

-spec read(pid() | atom(), query()) -> [] | {ok, pid()}.
read(Connection, Request = #'query'{collection = Collection, batchsize = BatchSize}) ->
  case request_worker(Connection, Request) of
    {_, []} ->
      [];
    {Cursor, Batch} ->
      mc_cursor:start_link(Connection, Collection, Cursor, BatchSize, Batch)
  end.

-spec read_one(pid() | atom(), query()) -> undefined | map().
read_one(Connection, Request) ->
  {0, Docs} = request_worker(Connection, Request#'query'{batchsize = -1}),
  case Docs of
    [] -> undefined;
    [Doc | _] -> Doc
  end.

-spec request_worker(pid(), mongo_protocol:message()) -> ok | {non_neg_integer(), [map()]}.
request_worker(Connection, Request) ->  %request to worker
  Timeout = mc_utils:get_timeout(),
  reply(gen_server:call(Connection, Request, Timeout)).

-spec process_reply(map(), mc_worker_api:selector()) -> {boolean(), map()}.
process_reply(Doc = #{<<"ok">> := N}, _) when is_number(N) ->   %command succeed | failed
  {N == 1, maps:remove(<<"ok">>, Doc)};
process_reply(Doc, Command) -> %unknown result
  erlang:error({bad_command, Doc}, [Command]).

-spec read_one_sync(port(), mc_worker_api:database(), mongo_protocol:message(), module()) -> map().
read_one_sync(Socket, Database, Request, SetOpts) ->
  {0, Docs} = request_raw(Socket, Database, Request#'query'{batchsize = -1}, SetOpts),
  case Docs of
    [] -> #{};
    [Doc | _] -> Doc
  end.


%% @private
reply(ok) -> ok;
reply(#reply{cursornotfound = false, queryerror = false} = Reply) ->
  {Reply#reply.cursorid, Reply#reply.documents};
reply(#reply{cursornotfound = false, queryerror = true} = Reply) ->
  [Doc | _] = Reply#reply.documents,
  process_error(maps:get(<<"code">>, Doc), Doc);
reply(#reply{cursornotfound = true, queryerror = false} = Reply) ->
  erlang:error({bad_cursor, Reply#reply.cursorid});
reply({error, Error}) ->
  process_error(error, Error).

%% @private
-spec process_error(atom() | integer(), term()) -> no_return().
process_error(?NOT_MASTER_ERROR, _) ->
  erlang:error(not_master);
process_error(Code, _) when ?UNAUTHORIZED_ERROR(Code) ->
  erlang:error(unauthorized);
process_error(_, Doc) ->
  erlang:error({bad_query, Doc}).

%% @private
-spec request_raw(port(), mc_worker_api:database(), mongo_protocol:message(), module()) ->
  ok | {non_neg_integer(), [map()]}.
request_raw(Socket, Database, Request, NetModule) ->
  Timeout = mc_utils:get_timeout(),
  ok = set_opts(Socket, NetModule, false),
  {ok, _, _} = mc_worker_logic:make_request(Socket, NetModule, Database, Request),
  Responses = recv_all(Socket, Timeout, NetModule),
  ok = set_opts(Socket, NetModule, true),
  {_, Reply} = hd(Responses),
  reply(Reply).

%% @private
set_opts(Socket, ssl, Value) ->
  ssl:setopts(Socket, [{active, Value}]);
set_opts(Socket, gen_tcp, Value) ->
  inet:setopts(Socket, [{active, Value}]).

%% @private
recv_all(Socket, Timeout, NetModule) ->
  recv_all(Socket, Timeout, NetModule, <<>>).
recv_all(Socket, Timeout, NetModule, Rest) ->
  {ok, Packet} = NetModule:recv(Socket, 0, Timeout),
  case mc_worker_logic:decode_responses(<<Rest/binary, Packet/binary>>) of
    {[], Unfinished} -> recv_all(Socket, Timeout, NetModule, Unfinished);
    {Responses, _} -> Responses
  end.
