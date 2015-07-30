%% API for standalone mongo client. You get connection pid of gen_server via connect/2
%% and then pass it to all functions

-module(mongo).

-include("mongo_protocol.hrl").

-export([
  connect/1,
  disconnect/1,
  insert/3,
  update/4,
  update/5,
  delete/3,
  delete_one/3]).
-export([
  find_one/3,
  find_one/4,
  find/3,
  find/4
]).
-export([
  count/3,
  count/4
]).
-export([
  command/2,
  sync_command/3,
  ensure_index/3
]).

-type collection() :: binary() | atom(). % without db prefix
-type cursorid() :: integer().
-type selector() :: bson:document().
-type projector() :: bson:document().
-type skip() :: integer().
-type batchsize() :: integer(). % 0 = default batch size. negative closes cursor
-type modifier() :: bson:document().
-type connection() :: pid().
-type database() :: binary() | atom().
-type args() :: [arg()].
-type arg() :: {database, database()} | {login, binary()} | {password, binary()} | {w_mode, write_mode()} | {r_mode, read_mode()}.
-type write_mode() :: unsafe | safe | {safe, bson:document()}.
-type read_mode() :: master | slave_ok.
-type service() :: {Host :: inet:hostname() | inet:ip_address(), Post :: 0..65535}.
-type options() :: [option()].
-type option() :: {timeout, timeout()} | {ssl, boolean()} | ssl | {database, database()} | {read_mode, read_mode()} | {write_mode, write_mode()}.
-type cursor() :: pid().

-export_type([
  connection/0,
  service/0,
  options/0,
  args/0,
  cursorid/0,
  projector/0,
  selector/0,
  skip/0,
  batchsize/0,
  modifier/0]).


%% @doc Make one connection to server, return its pid
-spec connect(args()) -> {ok, pid()}.
connect(Args) ->
  mc_worker:start_link(Args).

-spec disconnect(pid()) -> ok.
disconnect(Connection) ->
  mc_worker:disconnect(Connection).

%% @doc Insert a document or multiple documents into a collection.
%%      Returns the document or documents with an auto-generated _id if missing.
-spec insert(pid(), collection(), A) -> A.
insert(Connection, Coll, Doc) when is_tuple(Doc) ->
  hd(insert(Connection, Coll, [Doc]));
insert(Connection, Coll, Docs) ->
  Converted = prepare_and_assign(Docs),
  mc_connection_man:request_async(Connection, #insert{collection = Coll, documents = Converted}),
  Converted.

%% @doc Replace the document matching criteria entirely with the new Document.
-spec update(pid(), collection(), selector(), bson:document()) -> ok.
update(Connection, Coll, Selector, Doc) ->
  update(Connection, Coll, Selector, Doc, []).

%% @doc Replace the document matching criteria entirely with the new Document.
-spec update(pid(), collection(), selector(), bson:document(), boolean()) -> ok.
update(Connection, Coll, Selector, Doc, Args) ->
  Upsert = mc_utils:get_value(projector, Args, false),
  MultiUpdate = mc_utils:get_value(projector, Args, false),
  Converted = prepare_and_assign(Doc),
  mc_connection_man:request_async(Connection, #update{collection = Coll, selector = Selector,
    updater = Converted, upsert = Upsert, multiupdate = MultiUpdate}).

%% @doc Delete selected documents
-spec delete(pid(), collection(), selector()) -> ok.
delete(Connection, Coll, Selector) ->
  mc_connection_man:request_async(Connection, #delete{collection = Coll, singleremove = false, selector = Selector}).

%% @doc Delete first selected document.
-spec delete_one(pid(), collection(), selector()) -> ok.
delete_one(Connection, Coll, Selector) ->
  mc_connection_man:request_async(Connection, #delete{collection = Coll, singleremove = true, selector = Selector}).

%% @doc Return first selected document, if any
-spec find_one(pid(), collection(), selector()) -> {} | {bson:document()}.
find_one(Connection, Coll, Selector) ->
  find_one(Connection, Coll, Selector, []).

%% @doc Return first selected document, if any
-spec find_one(pid(), collection(), selector(), proplists:proplist()) -> {} | {bson:document()}.
find_one(Connection, Coll, Selector, Args) ->
  Projector = mc_utils:get_value(projector, Args, []),
  Skip = mc_utils:get_value(skip, Args, 0),
  mc_action_man:read_one(Connection, #'query'{
    collection = Coll,
    selector = Selector,
    projector = Projector,
    skip = Skip
  }).

%% @doc Return selected documents.
-spec find(pid(), collection(), selector()) -> cursor().
find(Connection, Coll, Selector) ->
  find(Connection, Coll, Selector, []).

%% @doc Return projection of selected documents.
%%      Empty projection [] means full projection.
-spec find(pid(), collection(), selector(), proplists:proplist()) -> cursor().
find(Connection, Coll, Selector, Args) ->
  Projector = mc_utils:get_value(projector, Args, []),
  Skip = mc_utils:get_value(skip, Args, 0),
  BatchSize = mc_utils:get_value(batchsize, Args, 0),
  mc_action_man:read(Connection, #'query'{
    collection = Coll,
    selector = Selector,
    projector = Projector,
    skip = Skip,
    batchsize = BatchSize
  }).

%@doc Count selected documents
-spec count(pid(), collection(), selector()) -> integer().
count(Connection, Coll, Selector) ->
  count(Connection, Coll, Selector, 0).

%@doc Count selected documents up to given max number; 0 means no max.
%     Ie. stops counting when max is reached to save processing time.
-spec count(pid(), collection(), selector(), integer()) -> integer().
count(Connection, Coll, Selector, Limit) ->
  CollStr = mc_utils:value_to_binary(Coll),
  {true, Doc} = command(Connection, case Limit =< 0 of
                                      true -> {count, CollStr, 'query', Selector};
                                      false -> {count, CollStr, 'query', Selector, limit, Limit}
                                    end),
  trunc(bson:at(<<"n">>, Doc)). % Server returns count as float

%% @doc Create index on collection according to given spec.
%%      The key specification is a bson documents with the following fields:
%%      key      :: bson document, for e.g. {field, 1, other, -1, location, 2d}, <strong>required</strong>
%%      name     :: bson:utf8()
%%      unique   :: boolean()
%%      dropDups :: boolean()
-spec ensure_index(pid(), collection(), bson:document()) -> ok.
ensure_index(Connection, Coll, IndexSpec) ->
  mc_connection_man:request_async(Connection, #ensure_index{collection = Coll, index_spec = IndexSpec}).

%% @doc Execute given MongoDB command and return its result.
-spec command(pid(), bson:document()) -> {boolean(), bson:document()}. % Action
command(Connection, Command) ->
  {Doc} = mc_action_man:read_one(Connection, #'query'{
    collection = '$cmd',
    selector = Command
  }),
  mc_connection_man:process_reply(Doc, Command).

%% @doc Execute MongoDB command in this thread
-spec sync_command(port(), binary(), bson:document()) -> {boolean(), bson:document()}.
sync_command(Socket, Database, Command) ->
  {Doc} = mc_action_man:read_one_sync(Socket, Database, #'query'{
    collection = '$cmd',
    selector = Command
  }),
  mc_connection_man:process_reply(Doc, Command).

%% @private
prepare_and_assign(Docs) when is_tuple(Docs) ->
  case element(1, Docs) of
    <<"$", _/binary>> -> Docs;  %command
    _ ->  %document
      case prepare_doc(Docs) of
        Res when is_tuple(Res) -> [Res];
        List -> List
      end
  end;
prepare_and_assign(Docs) ->
  case prepare_doc(Docs) of
    Res when not is_list(Res) -> [Res];
    List -> List
  end.

%% @private
%% Convert maps or proplists to bson
prepare_doc(Docs) when is_list(Docs) ->  %list of documents
  case mc_utils:is_proplist(Docs) of
    true -> prepare_doc(maps:from_list(Docs)); %proplist
    false -> lists:map(fun prepare_doc/1, Docs)
  end;
prepare_doc(Doc) ->
  assign_id(Doc).

%% @private
-spec assign_id(bson:document() | map()) -> bson:document().
assign_id(Map) when is_map(Map) ->
  case maps:is_key(<<"_id">>, Map) of
    true -> Map;
    false -> Map#{<<"_id">> => mongo_id_server:object_id()}
  end;
assign_id(Doc) ->
  case bson:lookup(<<"_id">>, Doc) of
    {} -> bson:update(<<"_id">>, mongo_id_server:object_id(), Doc);
    _Value -> Doc
  end.