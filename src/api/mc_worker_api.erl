%% API for standalone mongo client. You get connection pid of gen_server via connect/2
%% and then pass it to all functions

-module(mc_worker_api).

-include("mongo_types.hrl").
-include("mongo_protocol.hrl").

-export([
  connect/0,
  connect/1,
  disconnect/1,
  get_version/1,
  database/2,
  insert/3,
  update/4,
  update/6,
  delete/3,
  delete_one/3,
  delete_limit/4,
  insert/4,
  update/7,
  delete_limit/5]).

-export([
  find_one/3,
  find_one/4,
  find/3,
  find/4,
  find/2,
  find_one/2]).
-export([
  count/3,
  count/4,
  count/2]).
-export([
  command/2,
  command/3,
  ensure_index/3,
  prepare/2]).

%% @doc shortcut for connect/1 with default params.
connect() ->
  connect([]).

%% @doc Make one connection to server, return its pid
-spec connect(args()) -> {ok, pid()}.
connect(Args) ->  % TODO args as map
  {ok, Connection} = mc_worker:start_link(Args),
  Login = mc_utils:get_value(login, Args),
  Password = mc_utils:get_value(password, Args),
  case (Login /= undefined) and (Password /= undefined) of
    true ->
      AuthSource = mc_utils:get_value(auth_source, Args, <<"admin">>),
      Version = get_version(Connection),
      mc_auth_logic:auth(Connection, Version, AuthSource, Login, Password);
    false -> ok
  end,
  {ok, Connection}.

  -spec disconnect(pid()) -> ok.
disconnect(Connection) ->
  mc_worker:disconnect(Connection).

%% @doc Switch database
-spec database(pid(), database()) -> ok.
database(Connection, Database) ->
  mc_worker:database(Connection, Database).

%% Get server version.
-spec get_version(pid()) -> float().
get_version(Connection) ->
  {true, #{<<"version">> := Version}} = command(Connection, {<<"buildinfo">>, 1}),
  {VFloat, _} = string:to_float(binary_to_list(Version)),
  VFloat.

%% @doc Insert a document or multiple documents into a collection.
%%      Returns the document or documents with an auto-generated _id if missing.
-spec insert(pid(), collection(), bson:document()) -> {{boolean(), map()}, bson:document()};
    (pid(), collection(), map()) -> {{boolean(), map()}, map()};
    (pid(), collection(), list()) -> {{boolean(), map()}, list()}.
insert(Connection, Coll, Docs) ->
  insert(Connection, Coll, Docs, {<<"w">>, 1}).

-spec insert(pid(), collection(), bson:document(), bson:document()) -> {{boolean(), map()}, bson:document()};
    (pid(), collection(), map(), bson:document()) -> {{boolean(), map()}, map()};
    (pid(), collection(), list(), bson:document()) -> {{boolean(), map()}, list()}.
insert(Connection, Coll, Doc, WC) when is_tuple(Doc); is_map(Doc) ->
  {Res, [UDoc | _]} = insert(Connection, Coll, [Doc], WC),
  {Res, UDoc};
insert(Connection, Coll, Docs, WC) ->
  Converted = prepare(Docs, fun assign_id/1),
  {command(Connection, {<<"insert">>, Coll, <<"documents">>, Converted, <<"writeConcern">>, WC}), Converted}.

%% @doc Replace the document matching criteria entirely with the new Document.
-spec update(pid(), collection(), selector(), map() | bson:document()) -> {boolean(), map()}.
update(Connection, Coll, Selector, Doc) ->
  update(Connection, Coll, Selector, Doc, false, false).

%% @doc Replace the document matching criteria entirely with the new Document.
-spec update(pid(), collection(), selector(), map() | bson:document(), boolean(), boolean()) -> {boolean(), map()}.
update(Connection, Coll, Selector, Doc, Upsert, MultiUpdate) ->
  Converted = prepare(Doc, fun(D) -> D end),
  command(Connection, {<<"update">>, Coll, <<"updates">>,
    [#{<<"q">> => Selector, <<"u">> => Converted, <<"upsert">> => Upsert, <<"multi">> => MultiUpdate}]}).

%% @doc Replace the document matching criteria entirely with the new Document.
-spec update(pid(), collection(), selector(), map() | bson:document(), boolean(), boolean(), bson:document()) -> {boolean(), map()}.
update(Connection, Coll, Selector, Doc, Upsert, MultiUpdate, WC) ->
  Converted = prepare(Doc, fun(D) -> D end),
  command(Connection, {<<"update">>, Coll, <<"updates">>,
    [#{<<"q">> => Selector, <<"u">> => Converted, <<"upsert">> => Upsert, <<"multi">> => MultiUpdate}],
    <<"writeConcern">>, WC}).

%% @doc Delete selected documents
-spec delete(pid(), collection(), selector()) -> {boolean(), map()}.
delete(Connection, Coll, Selector) ->
  delete_limit(Connection, Coll, Selector, 0).

%% @doc Delete first selected document.
-spec delete_one(pid(), collection(), selector()) -> {boolean(), map()}.
delete_one(Connection, Coll, Selector) ->
  delete_limit(Connection, Coll, Selector, 1).

%% @doc Delete selected documents
-spec delete_limit(pid(), collection(), selector(), integer()) -> {boolean(), map()}.
delete_limit(Connection, Coll, Selector, N) ->
  command(Connection, {<<"delete">>, Coll, <<"deletes">>,
    [#{<<"q">> => Selector, <<"limit">> => N}]}).

%% @doc Delete selected documents
-spec delete_limit(pid(), collection(), selector(), integer(), bson:document()) -> {boolean(), map()}.
delete_limit(Connection, Coll, Selector, N, WC) ->
  command(Connection, {<<"delete">>, Coll, <<"deletes">>,
    [#{<<"q">> => Selector, <<"limit">> => N}], <<"writeConcern">>, WC}).

%% @doc Return first selected document, if any
-spec find_one(pid(), colldb(), selector()) -> map() | undefined.
find_one(Connection, Coll, Selector) ->
  find_one(Connection, Coll, Selector, #{}).

%% @doc Return first selected document, if any
-spec find_one(pid(), colldb(), selector(), map()) -> map() | undefined.
find_one(Connection, Coll, Selector, Args) ->
  Projector = maps:get(projector, Args, #{}),
  Skip = maps:get(skip, Args, 0),
  ReadPref = maps:get(readopts, Args, #{<<"mode">> => <<"primary">>}),
  find_one(Connection,
    #'query'{
      collection = Coll,
      selector = mongoc:append_read_preference(Selector, ReadPref),
      projector = Projector,
      skip = Skip
    }).

-spec find_one(pid() | atom(), query()) -> map() | undefined.
find_one(Connection, Query) when is_record(Query, query) ->
  mc_connection_man:read_one(Connection, Query).

%% @doc Return selected documents.
-spec find(pid(), colldb(), selector()) -> {ok, cursor()} | [].
find(Connection, Coll, Selector) ->
  find(Connection, Coll, Selector, #{}).

%% @doc Return projection of selected documents.
%%      Empty projection [] means full projection.
-spec find(pid(), colldb(), selector(), map()) -> {ok, cursor()} | [].
find(Connection, Coll, Selector, Args) ->
  Projector = maps:get(projector, Args, #{}),
  Skip = maps:get(skip, Args, 0),
  BatchSize = maps:get(batchsize, Args, 0),
  ReadPref = maps:get(readopts, Args, #{<<"mode">> => <<"primary">>}),
  find(Connection,
    #'query'{
      collection = Coll,
      selector = mongoc:append_read_preference(Selector, ReadPref),
      projector = Projector,
      skip = Skip,
      batchsize = BatchSize,
      slaveok = true,
      sok_overriden = true
    }).

-spec find(pid() | atom(), query()) -> {ok, cursor()} | [].
find(Connection, Query) when is_record(Query, query) ->
  case mc_connection_man:read(Connection, Query) of
    [] -> [];
    {ok, Cursor} when is_pid(Cursor) ->
      {ok, Cursor}
  end.

%% @doc Count selected documents
-spec count(pid(), collection(), selector()) -> integer().
count(Connection, Coll, Selector) ->
  count(Connection, Coll, Selector, #{}).

%% @doc Count selected documents up to given max number; 0 means no max.
%%     Ie. stops counting when max is reached to save processing time.
-spec count(pid(), collection(), selector(), map()) -> integer().
count(Connection, Coll, Selector, Args = #{limit := Limit}) when Limit > 0 ->
  ReadPref = maps:get(readopts, Args, #{<<"mode">> => <<"primary">>}),
  count(Connection, {<<"count">>, Coll, <<"query">>, Selector, <<"limit">>, Limit, <<"$readPreference">>, ReadPref});
count(Connection, Coll, Selector, Args) ->
  ReadPref = maps:get(readopts, Args, #{<<"mode">> => <<"primary">>}),
  count(Connection, {<<"count">>, Coll, <<"query">>, Selector, <<"$readPreference">>, ReadPref}).

-spec count(pid() | atom(), bson:document()) -> integer().
count(Connection, Query) ->
  {true, #{<<"n">> := N}} = command(Connection, Query),
  trunc(N). % Server returns count as float

%% @doc Create index on collection according to given spec.
%%      The key specification is a bson documents with the following fields:
%%      IndexSpec      :: bson document, for e.g. {field, 1, other, -1, location, 2d}, <strong>required</strong>
-spec ensure_index(pid(), colldb(), bson:document()) -> ok | {error, any()}.
ensure_index(Connection, Coll, IndexSpec) ->
  mc_connection_man:request_worker(Connection, #ensure_index{collection = Coll, index_spec = IndexSpec}).

%% @doc Execute given MongoDB command and return its result.
-spec command(pid(), selector()) -> {boolean(), map()} | {ok, cursor()}.
command(Connection, Command) -> mc_connection_man:command(Connection, Command).

command(Connection, Command, IsSlaveOk) -> mc_connection_man:command(Connection, Command, IsSlaveOk).

-spec prepare(tuple() | list() | map(), fun()) -> list().
prepare(Docs, AssignFun) when is_tuple(Docs) -> %bson
  case element(1, Docs) of
    <<"$", _/binary>> -> Docs;  %command
    _ ->  %document
      case prepare_doc(Docs, AssignFun) of
        Res when is_tuple(Res) -> [Res];
        List -> List
      end
  end;
prepare(Doc, AssignFun) when is_map(Doc), map_size(Doc) == 1 ->
  case maps:keys(Doc) of
    [<<"$", _/binary>>] -> Doc; %command
    _ ->  %document
      case prepare_doc(Doc, AssignFun) of
        Res when is_tuple(Res) -> [Res];
        List -> List
      end
  end;
prepare(Doc, AssignFun) when is_map(Doc) ->
  Keys = maps:keys(Doc),
  case [K || <<"$", _/binary>> = K <- Keys] of
    Keys -> Doc; % multiple commands
    _ ->  % document
      case prepare_doc(Doc, AssignFun) of
        Res when is_tuple(Res) -> [Res];
        List -> List
      end
  end;
prepare(Docs, AssignFun) when is_list(Docs) ->
  case prepare_doc(Docs, AssignFun) of
    Res when not is_list(Res) -> [Res];
    List -> List
  end.


%% @private
%% Convert maps or proplists to bson
prepare_doc(Docs, AssignFun) when is_list(Docs) ->  %list of documents
  case mc_utils:is_proplist(Docs) of
    true -> prepare_doc(maps:from_list(Docs), AssignFun); %proplist
    false -> lists:map(fun(Doc) -> prepare_doc(Doc, AssignFun) end, Docs)
  end;
prepare_doc(Doc, AssignFun) ->
  AssignFun(Doc).

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
