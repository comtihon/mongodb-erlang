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

-dialyzer({no_fail_call, query_to_op_msg_cmd/2}).

-define(NOT_MASTER_ERROR, 13435).
-define(UNAUTHORIZED_ERROR(C), C =:= 13; C =:= 10057; C =:= 16550).

%% API
-export([request_worker/2, process_reply/2]).
-export([read/2, read/3, read_one/2]).
-export([op_msg/2, op_msg_read_one/2, op_msg_raw_result/2]).
-export([command/2, command/3, database_command/3, database_command/4, request_raw_no_parse/4]).

%% Exported for testing
-ifdef(TEST).
-export([normalize_map_values/1]).
-endif.



-spec read(pid() | atom(), query()) -> [] | {ok, pid()}.
read(Connection, Request) -> read(Connection, Request, undefined).

-spec read(pid() | atom(), query() | op_msg_command(), undefined | mc_worker_api:batchsize()) -> [] | {ok, pid()}.
read(Connection, Request = #'query'{collection = Collection, batchsize = BatchSize, database = DB}, CmdBatchSize) ->
  read(Connection, Request, Collection, select_batchsize(CmdBatchSize, BatchSize), DB);
read(Connection, Request = #'op_msg_command'{database = DB, command_doc = ([{_, Collection} | _ ] = Fields)},
    _CmdBatchSize)  ->
  BatchSize = case lists:keyfind(<<"batchSize">>, 1, Fields) of
    {_, Size} -> Size;
    false -> 101
  end,
  read(Connection, Request, Collection, BatchSize, DB).

read(Connection, Request, Collection, BatchSize, DB) ->
  case request_worker(Connection, Request) of
    {_, []} ->
      [];
    {Cursor, Batch} ->
      mc_cursor:start_link(Connection, Collection, Cursor, BatchSize, Batch, DB);
    X ->
      erlang:error({error_unexpected_response, X})
  end.

-spec read_one(pid() | atom(), request()) -> undefined | map().
read_one(Connection, Request) ->
  {0, Docs} = request_worker(Connection, Request#'query'{batchsize = -1}),
  case Docs of
    [] -> undefined;
    [Doc | _] -> Doc
  end.

-spec command(pid(), mc_worker_api:selector()) -> {boolean(), map()}.
command(Connection, Query = #query{selector = Cmd}) ->
  QueryOrOpMsg = query_to_op_msg_cmd(mc_utils:use_legacy_protocol(Connection), Query),
  case determine_cursor(Cmd) of
    false ->
      legacy_command(mc_utils:use_legacy_protocol(Connection), Connection, QueryOrOpMsg);
    BatchSize ->
      case read(Connection, QueryOrOpMsg, BatchSize) of
        [] -> [];
        {ok, Cursor} when is_pid(Cursor) ->
          {ok, Cursor}
      end
  end;
command(Connection, Command) when not is_record(Command, query) ->
  command(Connection,
    #'query'{
      collection = <<"$cmd">>,
      selector = Command
    }).

legacy_command(true, Connection, Query) ->
  Doc = read_one(Connection, Query),
  process_reply(Doc, Query);
legacy_command(false, Connection, OpMsg) ->
  {true, mc_connection_man:op_msg_raw_result(Connection, OpMsg)}.

-spec query_to_op_msg_cmd(boolean(), mc_worker_api:selector() ) -> query() | op_msg_command().
query_to_op_msg_cmd(true, Query) ->
  Query#query{batchsize = -1};
query_to_op_msg_cmd(false, Query) ->
  #query{database = DB, slaveok = SlaveOk, selector = Selector} = Query,
  %% Handle maps and BSON documents differently for MongoDB 6.0+ compatibility
  NewSelector = case Selector of
    S when is_map(S) ->
      %% For maps, normalize any tuple values to maps (MongoDB 6.0+ compatibility)
      %% This handles cases where application code passes tuples like {key, value}
      NormalizedMap = normalize_map_values(S),
      %% Check if $readPreference is already present
      case {maps:is_key(<<"$readPreference">>, NormalizedMap), SlaveOk} of
        {true, _} -> NormalizedMap;  %% Already has $readPreference
        {false, true} ->
          %% Add $readPreference for slave reads
          NormalizedMap#{<<"$readPreference">> => #{<<"mode">> => <<"primaryPreferred">>}};
        {false, false} ->
          %% Primary is default, no change needed
          NormalizedMap
      end;
    _ ->
      %% For BSON documents (tuples), use the original logic
      Fields = bson:fields(Selector),
      case {lists:keyfind(<<"$readPreference">>, 1, Fields), SlaveOk} of
        {{<<"$readPreference">>, _}, _} -> Selector;
        {false, true} ->
          bson:merge(Fields, bson:document([{<<"$readPreference">>, #{<<"mode">> => <<"primaryPreferred">>}}]));
        {false, false} ->
          Fields
      end
  end,
  #'op_msg_command'{
    database = DB,
    command_doc = NewSelector
  }.

%% @private
%% Normalize map values by converting plain tuples to maps
%% This handles MongoDB 6.0+ compatibility where nested tuples in maps cause issues
normalize_map_values(Map) when is_map(Map) ->
  maps:map(fun(_K, V) -> normalize_value(V) end, Map).

%% @private
normalize_value({}) -> #{};  %% Empty tuple -> empty map
normalize_value({K, V}) when is_atom(K); is_binary(K) ->
  %% Single key-value tuple -> map
  Key = if is_atom(K) -> atom_to_binary(K, utf8); true -> K end,
  #{Key => normalize_value(V)};
normalize_value(V) when is_map(V) ->
  %% Recursively normalize nested maps
  normalize_map_values(V);
normalize_value(V) when is_list(V) ->
  %% Check if it's a proplist or just a list
  case is_proplist(V) of
    true -> maps:from_list([{ensure_binary(K), normalize_value(Val)} || {K, Val} <- V]);
    false -> V  %% Regular list, leave as-is
  end;
normalize_value(V) -> V.  %% Other values pass through unchanged

%% @private
is_proplist([]) -> true;
is_proplist([{K, _V} | Rest]) when is_atom(K); is_binary(K) -> is_proplist(Rest);
is_proplist(_) -> false.

%% @private
ensure_binary(A) when is_atom(A) -> atom_to_binary(A, utf8);
ensure_binary(B) when is_binary(B) -> B.

command(Connection, Command, _IsSlaveOk = true) ->
  command(Connection,
    #'query'{
      collection = <<"$cmd">>,
      selector = Command,
      slaveok = true,
      sok_overriden = true
    });
command(Connection, Command, _IsSlaveOk = false) ->
  command(Connection, Command).

-spec database_command(pid(), database(), selector()) -> {boolean(), map()} | {ok, cursor()}.
database_command(Connection, Database, Command) ->
  command(Connection,
    #'query'{
      collection = <<"$cmd">>,
      selector = Command,
      database = Database
    }).

-spec database_command(pid(), database(), selector(), boolean()) -> {boolean(), map()} | {ok, cursor()}.
database_command(Connection, Database, Command, IsSlaveOk) ->
  command(Connection,
    #'query'{
      collection = <<"$cmd">>,
      selector = Command,
      database = Database
    },
    IsSlaveOk).

-spec request_worker(pid(), mongo_protocol:message()) -> ok | {non_neg_integer(), [map()]} | map().
request_worker(Connection, Request) ->  %request to worker
  Timeout = mc_utils:get_timeout(),
  reply(gen_server:call(Connection, Request, Timeout)).

-spec process_reply(map(), mc_worker_api:selector()) -> {boolean(), map()}.
process_reply(Doc = #{<<"ok">> := N}, _) when is_number(N) ->   %command succeed | failed
  {N == 1, maps:remove(<<"ok">>, Doc)};
process_reply(Doc, Command) -> %unknown result
  erlang:error({bad_command, Doc}, [Command]).

op_msg(Connection, OpMsg) ->
  Doc = request_worker(Connection, OpMsg),
  process_reply(Doc, OpMsg).

op_msg_read_one(Connection, OpMsg) ->
  Timeout = mc_utils:get_timeout(),
  Response = gen_server:call(Connection, OpMsg, Timeout),
  case Response of
    #op_msg_response{response_doc =
    #{<<"ok">> := 1.0,
      <<"cursor">>:=
      #{<<"firstBatch">>:=[Doc],
        <<"id">>:=0}
    }} ->
      Doc;
    #op_msg_response{response_doc =
    #{<<"ok">> := 1.0}} ->
      undefined;
    #op_msg_response{response_doc = Doc} ->
      erlang:error({error, Doc});
    _ ->
      erlang:error({error_unexpected_response, Response})
  end.

op_msg_raw_result(Connection, OpMsg) ->
  Timeout = mc_utils:get_timeout(),
  FromServer = gen_server:call(Connection, OpMsg, Timeout),
  case FromServer of
    #op_msg_response{response_doc =
    (#{<<"ok">> := 1.0} = Res)} ->
      Res;
    _ ->
      erlang:error({error, FromServer})
  end.

request_raw_no_parse(Socket, Database, Request, NetModule) ->
  Timeout = mc_utils:get_timeout(),
  ok = set_opts(Socket, NetModule, false),
  {ok, _, _} = mc_worker_logic:make_request(Socket, NetModule, Database, Request),
  Result = recv_all(Socket, Timeout, NetModule),
  ok = set_opts(Socket, NetModule, true),
  Result.

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
  process_error(error, Error);
reply(#op_msg_response{response_doc = (#{<<"cursor">> := #{<<"firstBatch">> := Batch, <<"id">> := Id}} = Doc)}) when
  map_get(<<"ok">>, Doc) == 1 ->
  {Id, Batch};
reply(#op_msg_response{response_doc = Document}) when map_get(<<"ok">>, Document) == 1 ->
  Document;
reply(#op_msg_response{response_doc = Document}) ->
  %% Handle error responses (ok != 1)
  Code = maps:get(<<"code">>, Document, unknown_error),
  process_error(Code, Document);
reply(Resp) ->
  erlang:error({error_cannot_parse_response, Resp}).

%% @private
-spec process_error(atom() | integer(), term()) -> no_return().
process_error(?NOT_MASTER_ERROR, _) ->
  erlang:error(not_master);
process_error(Code, _) when ?UNAUTHORIZED_ERROR(Code) ->
  erlang:error(unauthorized);
process_error(_, Doc) ->
  erlang:error({bad_query, Doc}).

%% @private
select_batchsize(undefined, Batchsize) -> Batchsize;
select_batchsize(Batchsize, _) -> Batchsize.

%% @private
determine_cursor(#{<<"cursor">> := Cursor}) -> find_batchsize(Cursor);
determine_cursor(Cmd) when is_tuple(Cmd) ->
  bson:doc_foldl(
    fun
      (<<"cursor">>, Cursor, _) ->
        find_batchsize(Cursor);
      (_, _, Acc) ->
        Acc
    end, false, Cmd);
determine_cursor(_) -> false.

%% @private
find_batchsize(#{<<"batchSize">> := Batchsize}) -> Batchsize;
find_batchsize(Bson) when is_tuple(Bson) ->
  case bson:at(<<"batchSize">>, Bson) of
    null -> 101;
    V -> V
  end;
find_batchsize(_) -> 101.
