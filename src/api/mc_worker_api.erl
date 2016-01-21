%% API for standalone mongo client. You get connection pid of gen_server via connect/2
%% and then pass it to all functions

-module(mc_worker_api).

-include("mongo_protocol.hrl").

-export([
  connect/1,
  disconnect/1,
  insert/3,
  update/4,
  update/6,
  delete/3,
  delete_one/3, delete_limit/4]).
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
  sync_command/4,
  ensure_index/3
  , prepare_and_assign/1]).


-type cursorid() :: integer().
-type selector() :: bson:document().
-type projector() :: bson:document().
-type skip() :: integer().
-type batchsize() :: integer(). % 0 = default batch size. negative closes cursor
-type modifier() :: bson:document().
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
  modifier/0,
  write_mode/0,
  read_mode/0,
  cursor/0]).


%% @doc Make one connection to server, return its pid
-spec connect(args()) -> {ok, pid()}.
connect(Args) ->
  mc_worker:start_link(Args).

-spec disconnect(pid()) -> ok.
disconnect(Connection) ->
  mc_worker:disconnect(Connection).

%% @doc Insert a document or multiple documents into a collection.
%%      Returns the document or documents with an auto-generated _id if missing.
-spec insert(pid(), colldb(), list() | map() | bson:document()) -> {{true | {error, any()}, list() | map()}, list()}.
insert(Connection, Coll, Doc) when is_tuple(Doc); is_map(Doc) ->
  {Res, [UDoc | _]} = insert(Connection, Coll, [Doc]),
  {Res, UDoc};
insert(Connection, Coll, Docs) ->
  Converted = prepare_and_assign(Docs),
  {command(Connection, {<<"insert">>, Coll, <<"documents">>, Converted}), Converted}.

%% @doc Replace the document matching criteria entirely with the new Document.
-spec update(pid(), colldb(), selector(), bson:document()) -> true | {error, any()}.
update(Connection, Coll, Selector, Doc) ->
  update(Connection, Coll, Selector, Doc, false, false).

%% @doc Replace the document matching criteria entirely with the new Document.
-spec update(pid(), colldb(), selector(), bson:document(), boolean(), boolean()) -> true | {error, any()}.
update(Connection, Coll, Selector, Doc, Upsert, MultiUpdate) ->
  Converted = prepare_and_assign(Doc),
  command(Connection, {<<"update">>, Coll, <<"updates">>,
    [#{<<"q">> => Selector, <<"u">> => Converted, <<"upsert">> => Upsert, <<"multi">> => MultiUpdate}]}).

%% @doc Delete selected documents
-spec delete(pid(), colldb(), selector()) -> true | {error, any()}.
delete(Connection, Coll, Selector) ->
  delete_limit(Connection, Coll, Selector, 0).

%% @doc Delete first selected document.
-spec delete_one(pid(), colldb(), selector()) -> true | {error, any()}.
delete_one(Connection, Coll, Selector) ->
  delete_limit(Connection, Coll, Selector, 1).

%% @doc Delete selected documents
-spec delete_limit(pid(), colldb(), selector(), integer()) -> true | {error, any()}.
delete_limit(Connection, Coll, Selector, N) ->
  command(Connection, {<<"delete">>, Coll, <<"deletes">>,
    [#{<<"q">> => Selector, <<"limit">> => N}]}).

%% @doc Return first selected document, if any
-spec find_one(pid(), colldb(), selector()) -> {} | {bson:document()}.
find_one(Connection, Coll, Selector) ->
  find_one(Connection, Coll, Selector, #{}).

%% @doc Return first selected document, if any
-spec find_one(pid(), colldb(), selector(), map()) -> {} | {bson:document()}.
find_one(Connection, Coll, Selector, Args) ->
  Projector = maps:get(projector, Args, []),
  Skip = maps:get(skip, Args, 0),
  mc_action_man:read_one(Connection, #'query'{
    collection = Coll,
    selector = Selector,
    projector = Projector,
    skip = Skip
  }).

%% @doc Return selected documents.
-spec find(pid(), colldb(), selector()) -> cursor().
find(Connection, Coll, Selector) ->
  find(Connection, Coll, Selector, #{}).

%% @doc Return projection of selected documents.
%%      Empty projection [] means full projection.
-spec find(pid(), colldb(), selector(), map()) -> cursor().
find(Connection, Coll, Selector, Args) ->
  Projector = maps:get(projector, Args, []),
  Skip = maps:get(skip, Args, 0),
  BatchSize = maps:get(batchsize, Args, 0),
  mc_action_man:read(Connection, #'query'{
    collection = Coll,
    selector = Selector,
    projector = Projector,
    skip = Skip,
    batchsize = BatchSize
  }).

%% @doc Count selected documents
-spec count(pid(), colldb(), selector()) -> integer().
count(Connection, Coll, Selector) ->
  count(Connection, Coll, Selector, 0).

%% @doc Count selected documents up to given max number; 0 means no max.
%%     Ie. stops counting when max is reached to save processing time.
-spec count(pid(), colldb(), selector(), integer()) -> integer().
count(Connection, Coll, Selector, Limit) when not is_binary(Coll) ->
  count(Connection, mc_utils:value_to_binary(Coll), Selector, Limit);
count(Connection, Coll, Selector, Limit) when Limit =< 0 ->
  {true, #{<<"n">> := N}} = command(Connection, {<<"count">>, Coll, <<"query">>, Selector}),
  trunc(N);
count(Connection, Coll, Selector, Limit) ->
  {true, #{<<"n">> := N}} = command(Connection, {<<"count">>, Coll, <<"query">>, Selector, <<"limit">>, Limit}),
  trunc(N). % Server returns count as float

%% @doc Create index on collection according to given spec.
%%      The key specification is a bson documents with the following fields:
%%      key      :: bson document, for e.g. {field, 1, other, -1, location, 2d}, <strong>required</strong>
%%      name     :: bson:utf8()
%%      unique   :: boolean()
%%      dropDups :: boolean()
-spec ensure_index(pid(), colldb(), bson:document()) -> ok | {error, any()}.
ensure_index(Connection, Coll, IndexSpec) ->
  mc_connection_man:request_worker(Connection, #ensure_index{collection = Coll, index_spec = IndexSpec}).

%% @doc Execute given MongoDB command and return its result.
-spec command(pid(), bson:document()) -> {boolean(), bson:document()}. % Action
command(Connection, Command) ->
  Doc = mc_action_man:read_one(Connection, #'query'{
    collection = <<"$cmd">>,
    selector = Command
  }),
  mc_connection_man:process_reply(Doc, Command).

%% @doc Execute MongoDB command in this thread
-spec sync_command(port(), binary(), bson:document(), module()) -> {boolean(), bson:document()}.
sync_command(Socket, Database, Command, SetOpts) ->
  Doc = mc_action_man:read_one_sync(Socket, Database, #'query'{
    collection = <<"$cmd">>,
    selector = Command
  }, SetOpts),
  mc_connection_man:process_reply(Doc, Command).

prepare_and_assign(Docs) when is_tuple(Docs) ->
  case element(1, Docs) of
    <<"$", _/binary>> -> Docs;  %command
    _ ->  %document
      case prepare_doc(Docs) of
        Res when is_tuple(Res) -> [Res];
        List -> List
      end
  end;
prepare_and_assign(Doc) when is_map(Doc), map_size(Doc) == 1 ->
  case maps:keys(Doc) of
    [<<"$", _/binary>>] -> Doc; %command
    _ ->  %document
      case prepare_doc(Doc) of
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