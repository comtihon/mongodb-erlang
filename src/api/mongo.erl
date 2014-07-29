%% API for standalone mongo client. You get connection pid of gen_server via connect/2
%% and then pass it to all functions

-module(mongo).
-export([
	auth/3,
	connect/3,
	connect/4,
	connect/6,  %TODO disconnect?
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

-include("mongo_protocol.hrl").

-type cursor() :: pid().

%% @doc Make one connection to server, return its pid
-spec connect(Host :: string(), Port :: integer(), Database :: string()) -> {ok, Pid :: pid()}.
connect(Host, Port, Database) ->
	mc_worker:start_link({Host, Port, #conn_state{database = Database}}, []).
-spec connect(Host :: string(), Port :: integer(), Database :: string(), Opts :: proplists:proplist()) -> {ok, Pid :: pid()}.
connect(Host, Port, Database, Opts) ->
	mc_worker:start_link({Host, Port, #conn_state{database = Database}}, Opts).
-spec connect(Host :: string(), Port :: integer(), Database :: string(),
		Wmode :: write_mode(), Rmode :: read_mode(), Opts :: proplists:proplist()) -> Pid :: pid().
connect(Host, Port, Database, Wmode, Rmode, Opts) ->
	mc_worker:start_link({Host, Port, #conn_state{database = Database, write_mode = Wmode, read_mode = Rmode}}, Opts).

%% @doc Auth to MongoDB
-spec auth(Connection :: pid(), bson:utf8(), bson:utf8()) -> bson:document().
auth(Connection, Username, Password) ->
	{true, Res} = command(Connection, {getnonce, 1}),
	Nonce = bson:at(nonce, Res),
	command(Connection, {authenticate, 1, user, Username, nonce, Nonce, key, mc_connection_man:pw_key(Nonce, Username, Password)}).

%% @doc Insert a document or multiple documents into a collection.
%%      Returns the document or documents with an auto-generated _id if missing.
-spec insert(pid(), collection(), A) -> A.
insert(Connection, Coll, Doc) when is_tuple(Doc) ->
	hd(insert(Connection, Coll, [Doc]));
insert(Connection, Coll, Docs) ->
	Docs1 = [assign_id(Doc) || Doc <- Docs],
	mc_connection_man:request(Connection, #insert{collection = Coll, documents = Docs1}),
	Docs1.

%% @doc Replace the document matching criteria entirely with the new Document.
-spec update(pid(), collection(), selector(), bson:document()) -> ok.
update(Connection, Coll, Selector, Doc) ->
	update(Connection, Coll, Selector, Doc, false, false).

%% @doc Replace the document matching criteria entirely with the new Document.
-spec update(pid(), collection(), selector(), bson:document(), boolean()) -> ok.
update(Connection, Coll, Selector, Doc, Upsert) ->
	update(Connection, Coll, Selector, Doc, Upsert, false).

%% @doc Replace the document matching criteria entirely with the new Document.
-spec update(pid(), collection(), selector(), bson:document(), boolean(), boolean()) -> ok.
update(Connection, Coll, Selector, Doc, Upsert, MultiUpdate) ->
	mc_connection_man:request(Connection, #update{collection = Coll, selector = Selector, updater = Doc, upsert = Upsert, multiupdate = MultiUpdate}).

%% @doc Delete selected documents
-spec delete(pid(), collection(), selector()) -> ok.
delete(Connection, Coll, Selector) ->
	mc_connection_man:request(Connection, #delete{collection = Coll, singleremove = false, selector = Selector}).

%% @doc Delete first selected document.
-spec delete_one(pid(), collection(), selector()) -> ok.
delete_one(Connection, Coll, Selector) ->
	mc_connection_man:request(Connection, #delete{collection = Coll, singleremove = true, selector = Selector}).

%% @doc Return first selected document, if any
-spec find_one(pid(), collection(), selector()) -> {} | {bson:document()}.
find_one(Connection, Coll, Selector) ->
	find_one(Connection, Coll, Selector, []).

%% @doc Return projection of first selected document, if any. Empty projection [] means full projection.
-spec find_one(pid(), collection(), selector(), projector()) -> {} | {bson:document()}.
find_one(Connection, Coll, Selector, Projector) ->
	find_one(Connection, Coll, Selector, Projector, 0).

%% @doc Return projection of Nth selected document, if any. Empty projection [] means full projection.
-spec find_one(pid(), collection(), selector(), projector(), skip()) -> {} | {bson:document()}.
find_one(Connection, Coll, Selector, Projector, Skip) ->
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
-spec find(pid(), collection(), selector(), projector()) -> cursor().
find(Connection, Coll, Selector, Projector) ->
	find(Connection, Coll, Selector, Projector, 0).

%% @doc Return projection of selected documents starting from Nth document.
%%      Empty projection means full projection.
-spec find(pid(), collection(), selector(), projector(), skip()) -> cursor().
find(Connection, Coll, Selector, Projector, Skip) ->
	find(Connection, Coll, Selector, Projector, Skip, 0).

%% @doc Return projection of selected documents starting from Nth document in batches of batchsize.
%%      0 batchsize means default batch size.
%%      Negative batch size means one batch only.
%%      Empty projection means full projection.
-spec find(pid(), collection(), selector(), projector(), skip(), batchsize()) -> cursor(). % Action
find(Connection, Coll, Selector, Projector, Skip, BatchSize) ->
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
	CollStr = mc_connection_man:value_to_binary(Coll),
	{true, Doc} = command(Connection, case Limit =< 0 of
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
-spec ensure_index(pid(), collection(), bson:document()) -> ok.
ensure_index(Connection, Coll, IndexSpec) ->
	mc_connection_man:request(Connection, #ensure_index{collection = Coll, index_spec = IndexSpec}).

%% @doc Execute given MongoDB command and return its result.
-spec command(pid(), bson:document()) -> bson:document(). % Action
command(Connection, Command) ->
	{Doc} = mc_action_man:read_one(Connection, #'query'{
		collection = '$cmd',
		selector = Command
	}),
	mc_connection_man:process_reply(Doc, Command).


%% @private
-spec assign_id(bson:document()) -> bson:document().
assign_id(Doc) ->
	case bson:lookup('_id', Doc) of
		{_Value} -> Doc;
		{} -> bson:update('_id', mongo_id_server:object_id(), Doc)
	end.