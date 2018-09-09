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
-export([request_worker/2]).
-export([read/2, read/3, read_one/2]).
-export([command/2, command/3, database_command/3, database_command/4]).

-spec read(pid() | atom(), query()) -> [] | {ok, pid()}.
read(Connection, Request) -> read(Connection, Request, undefined).

-spec read(pid() | atom(), query(), undefined | mc_worker_api:batchsize()) -> [] | {ok, pid()}.
read(Connection, Request = #'query'{collection = Collection, batchsize = BatchSize}, CmdBatchSize) ->
  case request_worker(Connection, Request) of
    {_, []} ->
      [];
    {Cursor, Batch} ->
      mc_cursor:start_link(Connection, Collection, Cursor, select_batchsize(CmdBatchSize, BatchSize), Batch)
  end.

-spec read_one(pid() | atom(), query()) -> undefined | map().
read_one(Connection, Request) ->
  {0, Docs} = request_worker(Connection, Request#'query'{batchsize = -1}),
  case Docs of
    [] -> undefined;
    [Doc | _] -> Doc
  end.

command(Connection, Query = #query{selector = Cmd}) ->
  case determine_cursor(Cmd) of
    false ->
      Doc = read_one(Connection, Query),
      process_reply(Doc, Query);
    BatchSize ->
      case read(Connection, Query#query{batchsize = -1}, BatchSize) of
        [] -> [];
        {ok, Cursor} when is_pid(Cursor) ->
          {ok, Cursor}
      end
  end;
command(Connection, Command) when not is_record(Command, query)->
  command(Connection,
    #'query'{
      collection = <<"$cmd">>,
      selector = Command
    }).

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

-spec request_worker(pid(), mongo_protocol:message()) -> ok | {non_neg_integer(), [map()]}.
request_worker(Connection, Request) ->  %request to worker
  Timeout = mc_utils:get_timeout(),
  reply(gen_server:call(Connection, Request, Timeout)).

-spec process_reply(map(), mc_worker_api:selector()) -> {boolean(), map()}.
process_reply(Doc = #{<<"ok">> := N}, _) when is_number(N) ->   %command succeed | failed
  {N == 1, maps:remove(<<"ok">>, Doc)};
process_reply(Doc, Command) -> %unknown result
  erlang:error({bad_command, Doc}, [Command]).


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
