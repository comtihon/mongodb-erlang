%% API for standalone mongo client. You get connection pid of gen_server via connect/2
%% and then pass it to all functions

-module(mongo).
-export([
	connect/2,
	insert/3,
	update/4,
	update/5,
	update/6,
	delete/3,
	delete_one/3]).
-export([
]).
-export([
	find_one/3,
	find_one/4,
	find_one/5,
	find/3,
	find/4,
	find/5,
	find/6
]).
-export([
	count/3,
	count/4
]).
-export([
	command/2,
	ensure_index/3
]).
% TODO: add auth/2

-include("mongo_protocol.hrl").

-type cursor() :: pid().

%% @doc Make one connection to server, return its pid
-spec connect(Host :: inet:ip_address() | inet:hostname(), Port :: inet:port_number()) -> Pid :: pid().
connect(Host, Port) ->
	mongo_connection_worker:start_link({Host, Port}).

%% @doc Insert a document or multiple documents into a collection.
%%      Returns the document or documents with an auto-generated _id if missing.
-spec insert(pid()|term(), collection(), A) -> A.
insert(Coll, Doc, Connection) when is_tuple(Doc) ->
	hd(insert(Coll, [Doc], Connection));
insert(Coll, Docs, Connection) ->
	Docs1 = [assign_id(Doc) || Doc <- Docs],
	mc_action_man:write(Connection, #insert{collection = Coll, documents = Docs1}),
	Docs1.

%% @doc Replace the document matching criteria entirely with the new Document.
-spec update(pid()|term(), collection(), selector(), bson:document()) -> ok.
update(Connection, Coll, Selector, Doc) ->
	update(Connection, Coll, Selector, Doc, false, false).

%% @doc Replace the document matching criteria entirely with the new Document.
-spec update(pid()|term(), collection(), selector(), bson:document(), boolean()) -> ok.
update(Connection, Coll, Selector, Doc, Upsert) ->
	update(Connection, Coll, Selector, Doc, Upsert, false).

%% @doc Replace the document matching criteria entirely with the new Document.
-spec update(pid()|term(), collection(), selector(), bson:document(), boolean(), boolean()) -> ok.
update(Connection, Coll, Selector, Doc, Upsert, MultiUpdate) ->
	mc_action_man:write(Connection, #update{collection = Coll, selector = Selector, updater = Doc, upsert = Upsert, multiupdate = MultiUpdate}).

%% @doc Delete selected documents
-spec delete(pid()|term(), collection(), selector()) -> ok.
delete(Connection, Coll, Selector) ->
	mc_action_man:write(Connection, #delete{collection = Coll, singleremove = false, selector = Selector}).

%% @doc Delete first selected document.
-spec delete_one(pid()|term(), collection(), selector()) -> ok.
delete_one(Connection, Coll, Selector) ->
	mc_action_man:write(Connection, #delete{collection = Coll, singleremove = true, selector = Selector}).

%% @doc Return first selected document, if any
-spec find_one(pid() | term(), collection(), selector()) -> {} | {bson:document()}.
find_one(Connection, Coll, Selector) ->
	find_one(Connection, Coll, Selector, []).

%% @doc Return projection of first selected document, if any. Empty projection [] means full projection.
-spec find_one(pid() | term(), collection(), selector(), projector()) -> {} | {bson:document()}.
find_one(Connection, Coll, Selector, Projector) ->
	find_one(Connection, Coll, Selector, Projector, 0).

%% @doc Return projection of Nth selected document, if any. Empty projection [] means full projection.
-spec find_one(pid() | term(), collection(), selector(), projector(), skip()) -> {} | {bson:document()}.
find_one(Connection, Coll, Selector, Projector, Skip) ->
	mc_action_man:read_one(Connection, #'query'{
		collection = Coll,
		selector = Selector,
		projector = Projector,
		skip = Skip
	}).

%% @doc Return selected documents.
-spec find(pid() | term(), collection(), selector()) -> cursor().
find(Connection, Coll, Selector) ->
	find(Connection, Coll, Selector, []).

%% @doc Return projection of selected documents.
%%      Empty projection [] means full projection.
-spec find(pid() | term(), collection(), selector(), projector()) -> cursor().
find(Connection, Coll, Selector, Projector) ->
	find(Connection, Coll, Selector, Projector, 0).

%% @doc Return projection of selected documents starting from Nth document.
%%      Empty projection means full projection.
-spec find(pid() | term(), collection(), selector(), projector(), skip()) -> cursor().
find(Connection, Coll, Selector, Projector, Skip) ->
	find(Connection, Coll, Selector, Projector, Skip, 0).

%% @doc Return projection of selected documents starting from Nth document in batches of batchsize.
%%      0 batchsize means default batch size.
%%      Negative batch size means one batch only.
%%      Empty projection means full projection.
-spec find(pid() | term(), collection(), selector(), projector(), skip(), batchsize()) -> cursor(). % Action
find(Connection, Coll, Selector, Projector, Skip, BatchSize) ->
	mc_action_man:read(Connection, #'query'{
		collection = Coll,
		selector = Selector,
		projector = Projector,
		skip = Skip,
		batchsize = BatchSize
	}).

%@doc Count selected documents
-spec count(pid() | term(), collection(), selector()) -> integer().
count(Connection, Coll, Selector) ->
	count(Connection, Coll, Selector, 0).

%@doc Count selected documents up to given max number; 0 means no max.
%     Ie. stops counting when max is reached to save processing time.
-spec count(pid() | term(), collection(), selector(), integer()) -> integer().
count(Connection, Coll, Selector, Limit) ->
	CollStr = atom_to_binary(Coll, utf8),
	Doc = command(Connection, case Limit =< 0 of
		                          true -> {count, CollStr, 'query', Selector};
		                          false -> {count, CollStr, 'query', Selector, limit, Limit}
	                          end),
	trunc(bson:at(n, Doc)). % Server returns count as float

%% @doc Create index on collection according to given spec.
%%      The key specification is a bson documents with the following fields:
%%      key      :: bson document, for e.g. {field, 1, other, -1, location, 2d}, <strong>required</strong>
%%      name     :: bson:utf8()
%%      unique   :: boolean()
%%      dropDups :: boolean()
-spec ensure_index(pid() | term(), collection(), bson:document()) -> ok.
ensure_index(Connection, Coll, IndexSpec) ->
	#context{database = Database} = erlang:get(mongo_action_context),
	Key = bson:at(key, IndexSpec),
	Defaults = {name, gen_index_name(Key), unique, false, dropDups, false},
	Index = bson:update(ns, mongo_protocol:dbcoll(Database, Coll), bson:merge(IndexSpec, Defaults)),
	insert(Connection, 'system.indexes', Index),
	ok.

%% @doc Execute given MongoDB command and return its result.
-spec command(pid() | term(), bson:document()) -> bson:document(). % Action
command(Connection, Command) ->
	{Doc} = mc_action_man:read_one(Connection, #'query'{
		collection = '$cmd',
		selector = Command
	}),
	case bson:at(ok, Doc) of
		true -> Doc;
		N when N == 1 -> Doc;
		_ -> erlang:error({bad_command, Doc}, [Command])
	end.


%% @private
-spec assign_id(bson:document()) -> bson:document().
assign_id(Doc) ->
	case bson:lookup('_id', Doc) of
		{_Value} -> Doc;
		{} -> bson:update('_id', mongo_id_server:object_id(), Doc)
	end.

%% @private
gen_index_name(KeyOrder) ->
	bson:doc_foldl(fun(Label, Order, Acc) ->
		<<Acc/binary, $_, (value_to_binary(Label))/binary, $_, (value_to_binary(Order))/binary>>
	end, <<"i">>, KeyOrder).

%% @private
value_to_binary(Value) when is_integer(Value) ->
	bson:utf8(integer_to_list(Value));
value_to_binary(Value) when is_atom(Value) ->
	atom_to_binary(Value, utf8);
value_to_binary(Value) when is_binary(Value) ->
	Value;
value_to_binary(_Value) ->
	<<>>.