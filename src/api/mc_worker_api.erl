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
  delete/1,
  delete/3,
  delete_one/3,
  delete_limit/4,
  delete_limit/5,
  delete_limit/6,
  insert/4,
  insert/5,
  insert/1,
  update/7,
  update/8,
  update/1]).

-export([
  find_one/3,
  find_one/4,
  find_one/5,
  find_one/1,
  find/3,
  find/4,
  find/5,
  find/2,
  find/1,
  find_one/2]).
-export([
  count/1,
  count/3,
  count/4,
  count/2]).
-export([
  command/2,
  command/3,
  command/4,
  ensure_index/3,
  ensure_index/4]).

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

%% @deprecated
%% @doc Insert a document or multiple documents into a collection.
%%      Returns the document or documents with an auto-generated _id if missing.
-spec insert(pid(), collection(), bson:document()) -> {{boolean(), map()}, bson:document()};
    (pid(), collection(), map()) -> {{boolean(), map()}, map()};
    (pid(), collection(), list()) -> {{boolean(), map()}, list()}.
insert(Connection, Coll, Docs) ->
  insert(Connection, Coll, Docs, {<<"w">>, 1}).

%% @deprecated
insert(Connection, Coll, Doc, WC) ->
  insert(Connection, Coll, Doc, WC, undefined).

%% @deprecated
-spec insert(pid(), collection(), bson:document(), bson:document(), database() | undefined) -> {{boolean(), map()}, bson:document()};
    (pid(), collection(), map(), bson:document(), database() | undefined) -> {{boolean(), map()}, map()};
    (pid(), collection(), list(), bson:document(), database() | undefined) -> {{boolean(), map()}, list()}.
insert(Connection, Coll, Doc, WC, DB) when is_tuple(Doc); is_map(Doc) ->
  {Res, [UDoc | _]} = insert(Connection, Coll, [Doc], WC, DB),
  {Res, UDoc};
insert(Connection, Coll, Docs, WC, DB) ->
  ConvertedDocs = prepare(Docs, fun assign_id/1),
  UseLegacyProtocol = mc_utils:use_legacy_protocol(Connection),
  insert(UseLegacyProtocol, Connection, Coll, WC, DB, ConvertedDocs).

insert(true, Connection, Coll, WriteConcern, DB, ConvertedDocs) ->
  Command =
    command(DB, Connection, {<<"insert">>, Coll, <<"documents">>, ConvertedDocs, <<"writeConcern">>, WriteConcern}),
  {Command, ConvertedDocs};
insert(false, Connection, Coll, WriteConcern, _DB, ConvertedDocs) ->
  Msg = #op_msg_write_op{
    command = insert,
    collection = Coll,
    extra_fields = [{<<"writeConcern">>, WriteConcern}],
    documents = ConvertedDocs},
  {mc_connection_man:op_msg(Connection, Msg), ConvertedDocs}.

%% @doc Insert one document or multiple documents into a collection.
%% params:
%%  connection - mc_worker pid
%%  collection - collection()
%%  doc - bson:document() or list
%%  database - insert in this database (optional)
%%  write_concern - bson:document()
insert(Cmd = #{connection := Connection, collection := Collection, doc := Doc}) ->
  WC = maps:get(write_concern, Cmd, {<<"w">>, 1}),
  DB = maps:get(database, Cmd, undefined),
  insert(Connection, Collection, Doc, WC, DB).

%% @deprecated
%% @doc Replace the document matching criteria entirely with the new Document.
-spec update(pid(), collection(), selector(), map() | bson:document()) -> {boolean(), map()}.
update(Connection, Coll, Selector, Doc) ->
  update(Connection, Coll, Selector, Doc, false, false).

%% @deprecated
%% @doc Replace the document matching criteria entirely with the new Document.
-spec update(pid(), collection(), selector(), map() | bson:document(), boolean(), boolean()) -> {boolean(), map()}.
update(Connection, Coll, Selector, Doc, Upsert, MultiUpdate) ->
  update(Connection, Coll, Selector, Doc, Upsert, MultiUpdate, {<<"w">>, 1}).

%% @deprecated
%% @doc Replace the document matching criteria entirely with the new Document.
-spec update(pid(), collection(), selector(), map() | bson:document(), boolean(), boolean(), bson:document()) -> {boolean(), map()}.
update(Connection, Coll, Selector, Doc, Upsert, MultiUpdate, WC) ->
  update(Connection, Coll, Selector, Doc, Upsert, MultiUpdate, WC, undefined).

%% @deprecated
%% @doc Replace the document matching criteria entirely with the new Document.
-spec update(pid(), collection(), selector(), map() | bson:document(), boolean(), boolean(), bson:document(), database()) -> {boolean(), map()}.
update(Connection, Coll, Selector, Doc, Upsert, MultiUpdate, WriteConcern, DB) ->
  ConvertedDocs = prepare(Doc, fun(D) -> D end),
  UseLegacyProtocol = mc_utils:use_legacy_protocol(Connection),
  update(UseLegacyProtocol, Connection, Coll, Selector, ConvertedDocs, Upsert, MultiUpdate, WriteConcern, DB).

update(true, Connection, Coll, Selector, ConvertedDocs, Upsert, MultiUpdate, WriteConcern, DB) ->
    command(DB, Connection, {<<"update">>, Coll, <<"updates">>, [#{<<"q">> => Selector, <<"u">> => ConvertedDocs,
      <<"upsert">> => Upsert, <<"multi">> => MultiUpdate}], <<"writeConcern">>, WriteConcern});
update(false, Connection, Coll, Selector, ConvertedDocs, Upsert, MultiUpdate, WriteConcern, _DB) ->
  Msg = #op_msg_write_op{
    command = update,
    collection = Coll,
    extra_fields = [{<<"writeConcern">>, WriteConcern}],
    documents_name = <<"updates">>,
    documents = [#{
      <<"q">> => Selector,
      <<"u">> => ConvertedDocs,
      <<"upsert">> => Upsert,
      <<"multi">> => MultiUpdate}]},
  mc_connection_man:op_msg(Connection, Msg).

%% @doc Replace the document matching criteria entirely with the new Document.
%% params:
%%  connection - mc_worker pid
%%  collection - collection()
%%  selector - selector()
%%  doc - bson:document() or list
%%  database - insert in this database (optional)
%%  upsert - boolean() do upsert
%%  multi - boolean() multiupdate
%%  write_concern - bson:document()
update(Cmd = #{connection := Connection, collection := Collection, selector := Selector, doc := Doc}) ->
  Upsert = maps:get(upsert, Cmd, false),
  MultiUpdate = maps:get(multi, Cmd, false),
  WC = maps:get(write_concern, Cmd, {<<"w">>, 1}),
  DB = maps:get(database, Cmd, undefined),
  update(Connection, Collection, Selector, Doc, Upsert, MultiUpdate, WC, DB).

%% @deprecated
%% @doc Delete selected documents
-spec delete(pid(), collection(), selector()) -> {boolean(), map()}.
delete(Connection, Coll, Selector) ->
  delete_limit(Connection, Coll, Selector, 0).

%% @deprecated
%% @doc Delete first selected document.
-spec delete_one(pid(), collection(), selector()) -> {boolean(), map()}.
delete_one(Connection, Coll, Selector) ->
  delete_limit(Connection, Coll, Selector, 1).

%% @deprecated
%% @doc Delete selected documents
-spec delete_limit(pid(), collection(), selector(), integer()) -> {boolean(), map()}.
delete_limit(Connection, Coll, Selector, Limit) ->
  delete_limit(Connection, Coll, Selector, Limit, undefined).

%% @deprecated
%% @doc Delete selected documents
-spec delete_limit(pid(), collection(), selector(), integer(), bson:document() | undefined) -> {boolean(), map()}.
delete_limit(Connection, Coll, Selector, Limit, undefined) ->
  delete_limit(Connection, Coll, Selector, Limit, {<<"w">>, 1}, undefined);
delete_limit(Connection, Coll, Selector, Limit, WriteConcern) ->
  delete_limit(Connection, Coll, Selector, Limit, WriteConcern, undefined).

%% @deprecated
%% @doc Delete selected documents
-spec delete_limit(pid(), collection(), selector(), integer(), bson:document(), database()) -> {boolean(), map()}.
delete_limit(Connection, Coll, Selector, Limit, WriteConcern, DB) ->
  UseLegacyProtocol = mc_utils:use_legacy_protocol(Connection),
  delete_limit(UseLegacyProtocol, Connection, Coll, Selector, Limit, WriteConcern, DB).

delete_limit(true, Connection, Coll, Selector, Limit, WriteConcern, DB) ->
  command(DB, Connection, {<<"delete">>, Coll, <<"deletes">>,
    [#{<<"q">> => Selector, <<"limit">> => Limit}], <<"writeConcern">>, WriteConcern});
delete_limit(false, Connection, Coll, Selector, Limit, WriteConcern, _DB) ->
  Msg = #op_msg_write_op{command = delete,
    collection = Coll,
    extra_fields = [{<<"writeConcern">>, WriteConcern}],
    documents_name = <<"deletes">>,
    documents = [#{<<"q">> => Selector,
      <<"limit">> => Limit}]},
  mc_connection_man:op_msg(Connection, Msg).

%% @doc Delete selected documents
%% params:
%%  connection - mc_worker pid
%%  collection - collection()
%%  selector - selector()
%%  database - insert in this database (optional)
%%  num - int() number of documents (optional). 0 = all (default)
%%  write_concern - bson:document()
delete(Cmd = #{connection := Connection, collection := Collection, selector := Selector}) ->
  DB = maps:get(database, Cmd, undefined),
  N = maps:get(num, Cmd, 0),
  WC = maps:get(write_concern, Cmd, {<<"w">>, 1}),
  delete_limit(Connection, Collection, Selector, N, WC, DB).

%% @doc Return first selected document, if any
-spec find_one(pid(), colldb(), selector()) -> map() | undefined.
find_one(Connection, Coll, Selector) ->
  find_one(Connection, Coll, Selector, #{}).

%% @doc Return first selected document, if any
-spec find_one(pid(), colldb(), selector(), map()) -> map() | undefined.
find_one(Connection, Coll, Selector, Args) ->
  find_one(Connection, Coll, Selector, Args, undefined).

%% @doc Return first selected document, if any
-spec find_one(pid(), colldb(), selector(), map(), database()) -> map() | undefined.
find_one(Connection, Coll, Selector, Args, Db) ->
  Projector = maps:get(projector, Args, #{}),
  Skip = maps:get(skip, Args, 0),
  ReadPref = maps:get(readopts, Args, #{<<"mode">> => <<"primary">>}),
  UseLegacyProtocol = mc_utils:use_legacy_protocol(Connection),
  find_one(UseLegacyProtocol, Connection, Coll, Selector, Projector, Skip, ReadPref, Db).

find_one(true, Connection, Coll, Selector, Projector, Skip, ReadPref, Db) ->
  find_one(Connection,
    #'query'{
      database = Db,
      collection = Coll,
      selector = mongoc:append_read_preference(Selector, ReadPref),
      projector = Projector,
      skip = Skip
    });
find_one(false, Connection, Coll, Selector, Projector, Skip, ReadPref, _Db) ->
  CommandDoc = [
    {<<"find">>, Coll},
    {<<"$readPreference">>, ReadPref},
    {<<"filter">>, Selector},
    {<<"projection">>, Projector},
    {<<"skip">>, Skip},
    {<<"batchSize">>, 1},
    {<<"limit">>, 1},
    {<<"singleBatch">>, true} %% Close cursor after first batch
  ],
  mc_connection_man:op_msg_read_one(Connection,
    #'op_msg_command'{
      command_doc = CommandDoc
    }).

%% @doc Return projection of selected documents.
%% params:
%%  connection - mc_worker pid
%%  collection - collection()
%%  selector - selector()
%%  database - insert in this database (optional)
%%  projector - bson:document() optional
%%  skip - int() optional
%%  readopts - bson:document() optional
find_one(Cmd = #{connection := Connection, collection := Collection, selector := Selector}) ->
  DB = maps:get(database, Cmd, undefined),
  find_one(Connection, Collection, Selector, Cmd, DB).

-spec find_one(pid() | atom(), query()) -> map() | undefined.
find_one(Connection, Query) when is_record(Query, query) ->
  UseLegacyProtocol = mc_utils:use_legacy_protocol(Connection),
  find_one_with_query_record(UseLegacyProtocol, Connection, Query).

find_one_with_query_record(true, Connection, Query) ->
  mc_connection_man:read_one(Connection, Query);
find_one_with_query_record(false, Connection, Query) ->
  #'query'{collection = Coll,
    skip = Skip,
    selector = Selector,
    projector = Projector} = Query,
  {RP, NewSelector, _} = mongoc:extract_read_preference(Selector),
  Args = #{projector => Projector,
    skip => Skip,
    readopts => RP},
find_one(Connection, Coll, NewSelector, Args).

%% @deprecated
%% @doc Return selected documents.
-spec find(pid(), colldb(), selector()) -> {ok, cursor()} | [].
find(Connection, Coll, Selector) ->
  find(Connection, Coll, Selector, #{}).

%% @deprecated
-spec find(pid(), colldb(), selector(), map()) -> {ok, cursor()} | [].
find(Connection, Coll, Selector, Args) ->
  find(Connection, Coll, Selector, Args, undefined).

%% @deprecated
%% @doc Return projection of selected documents.
%%      Empty projection [] means full projection.
-spec find(pid(), colldb(), selector(), map(), database()) -> {ok, cursor()} | [].
find(Connection, Coll, Selector, Args, Db) ->
  Projector = maps:get(projector, Args, #{}),
  Skip = maps:get(skip, Args, 0),
  BatchSize =
        case mc_utils:use_legacy_protocol(Connection) of
            true ->
                maps:get(batchsize, Args, 0);
            false ->
                maps:get(batchsize, Args, 101)
        end,
  ReadPref = maps:get(readopts, Args, #{<<"mode">> => <<"primary">>}),
  find(Connection,
    #'query'{
      database = Db,
      collection = Coll,
      selector = mongoc:append_read_preference(Selector, ReadPref),
      projector = Projector,
      skip = Skip,
      batchsize = BatchSize,
      slaveok = true,
      sok_overriden = true
    }).

%% @doc Return projection of selected documents.
%% params:
%%  connection - mc_worker pid
%%  collection - collection()
%%  selector - selector()
%%  database - insert in this database (optional)
%%  projector - bson:document() optional
%%  skip - int() optional
%%  batchsize - int() optional
%%  readopts - bson:document() optional
find(Cmd = #{connection := Connection, collection := Collection, selector := Selector}) ->
  DB = maps:get(database, Cmd, undefined),
  find(Connection, Collection, Selector, Cmd, DB).

-spec find(pid() | atom(), query()) -> {ok, cursor()} | [].
find(Connection, Query) when is_record(Query, query) ->
  FixedQuery = fixed_query(mc_utils:use_legacy_protocol(Connection), Query),
  case mc_connection_man:read(Connection, FixedQuery) of
    [] -> [];
    {ok, Cursor} when is_pid(Cursor) ->
      {ok, Cursor}
  end.

fixed_query(true, Query) ->
  Query;
fixed_query(false, Query) ->
  #'query'{collection = Coll,
    skip = Skip,
    selector = Selector,
    batchsize = BatchSize,
    projector = Projector} = Query,
  {ReadPref, NewSelector, OrderBy} = mongoc:extract_read_preference(Selector),
  %% We might need to do some transformations:
  %% See: https://github.com/mongodb/specifications/blob/master/source/find_getmore_killcursors_commands.rst#mapping-op-query-behavior-to-the-find-command-limit-and-batchsize-fields
  SingleBatch = BatchSize < 0,
  AbsBatchSize = erlang:abs(BatchSize),
  BatchSizeField = batch_size(AbsBatchSize =:= 0, AbsBatchSize),
  SingleBatchField = single_batch(SingleBatch),
  SortField = sort_field(OrderBy),
  CommandDoc = [
    {<<"find">>, Coll},
    {<<"$readPreference">>, ReadPref},
    {<<"filter">>, NewSelector},
    {<<"projection">>, Projector},
    {<<"skip">>, Skip}
  ] ++ SortField
    ++ BatchSizeField
    ++ SingleBatchField,
  #op_msg_command{command_doc = CommandDoc}.

batch_size(true, _BatchSize) -> [];
batch_size(false, BatchSize) -> [{<<"batchSize">>, BatchSize}].

single_batch(true) -> [];
single_batch(false = SingleBatch) -> [{<<"singleBatch">>, SingleBatch}].

sort_field(OrderBy) when is_map(OrderBy), map_size(OrderBy) =:= 0 -> [];
sort_field(OrderBy) -> [{<<"sort">>, OrderBy}].

%% @deprecated
%% @doc Count selected documents
-spec count(pid(), collection(), selector()) -> integer().
count(Connection, Coll, Selector) ->
  count(Connection, Coll, Selector, #{}).

%% @deprecated
%% @doc Count selected documents up to given max number; 0 means no max.
%%     Ie. stops counting when max is reached to save processing time.
-spec count(pid(), collection(), selector(), map()) -> integer().
count(Connection, Coll, Selector, Args = #{limit := Limit}) when Limit > 0 ->
  ReadPref = maps:get(readopts, Args, #{<<"mode">> => <<"primary">>}),
  count(Connection, {<<"count">>, Coll, <<"query">>, Selector, <<"limit">>, Limit, <<"$readPreference">>, ReadPref});
count(Connection, Coll, Selector, Args) ->
  ReadPref = maps:get(readopts, Args, #{<<"mode">> => <<"primary">>}),
  count(Connection, {<<"count">>, Coll, <<"query">>, Selector, <<"$readPreference">>, ReadPref}).

%% @deprecated
-spec count(pid() | atom(), bson:document()) -> integer().
count(Connection, Query) ->
  {true, #{<<"n">> := N}} = command(Connection, Query),
  trunc(N). % Server returns count as float

%% @doc Return projection of selected documents.
%% params:
%%  connection - mc_worker pid
%%  collection - collection()
%%  selector - selector()
%%  database - insert in this database (optional)
%%  limit - int() optional. 0 - no limit
%%  readopts - bson:document() optional
count(Cmd = #{connection := Connection, collection := Collection, selector := Selector}) ->
  ReadPref = maps:get(readopts, Cmd, #{<<"mode">> => <<"primary">>}),
  Limit = maps:get(limit, Cmd, 0),
  DB = maps:get(database, Cmd, undefined),
  {true, #{<<"n">> := N}} = command(DB, Connection,
    {<<"count">>, Collection, <<"query">>, Selector, <<"limit">>, Limit, <<"$readPreference">>, ReadPref}),
  trunc(N).

%% @doc Create index on collection according to given spec.
%%      The key specification is a bson documents with the following fields:
%%      IndexSpec      :: bson document, for e.g. {field, 1, other, -1, location, 2d}, <strong>required</strong>
-spec ensure_index(pid(), colldb(), bson:document()) -> ok | {error, any()}.
ensure_index(Connection, Coll, IndexSpec) ->
  ensure_index(Connection, Coll, IndexSpec, undefined).

-spec ensure_index(pid(), colldb(), bson:document(), database()) -> ok | {error, any()}.
ensure_index(Connection, Coll, IndexSpec, DB) ->
  ensure_index(mc_utils:use_legacy_protocol(Connection), Connection, Coll, IndexSpec, DB).

ensure_index(true, Connection, Coll, IndexSpec, DB) ->
  mc_connection_man:request_worker(Connection, #ensure_index{database = DB, collection = Coll, index_spec = IndexSpec});
ensure_index(false, Connection, Coll, IndexSpec, DB) ->
  command(DB, Connection, {<<"createIndexes">>, Coll, <<"indexes">>, IndexSpec}).

%% @doc Execute given MongoDB command and return its result.
-spec command(pid(), selector()) -> {boolean(), map()} | {ok, cursor()}.
command(Connection, Command) -> mc_connection_man:command(Connection, Command).

%% @doc Execute given MongoDB command on specific database and return its result.
-spec command(database(), pid(), selector()) -> {boolean(), map()} | {ok, cursor()}.
command(undefined, Connection, Command) ->
  command(Connection, Command);
command(Db, Connection, Command) ->
  mc_connection_man:database_command(Connection, Db, Command).

command(Db, Connection, Command, IsSlaveOk) ->
  mc_connection_man:database_command(Connection, Db, Command, IsSlaveOk).

%% @private
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
