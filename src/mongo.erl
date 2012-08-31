-module(mongo).
-export([
	do/5
]).
-export([
	insert/2,
	update/3,
	update/4,
	update/5,
	delete/2,
	delete_one/2
]).
-export([
	find_one/2,
	find_one/3,
	find_one/4,
	find/2,
	find/3,
	find/4,
	find/5
]).
-export([
	count/2,
	count/3
]).
-export ([
	command/1,
	ensure_index/2
]).
% TODO: add auth/2

-include("mongo_protocol.hrl").

-type connection() :: pid().
-type database()   :: atom().
-type cursor()     :: pid().
-type write_mode() :: unsafe | safe | {safe, bson:document()}.
-type read_mode()  :: master | slave_ok.
-type action(A)    :: fun (() -> A).

-record(context, {
	write_mode :: write_mode(),
	read_mode  :: read_mode(),
	connection :: mongo_connection:connection(),
	database   :: database()
}).


%% @doc Execute mongo action under given write_mode, read_mode, connection, and database.
-spec do(write_mode(), read_mode(), connection(), database(), action(A)) -> A.
do(WriteMode, ReadMode, Connection, Database, Action) ->
	PrevContext = erlang:get(mongo_action_context),
	erlang:put(mongo_action_context, #context{
		write_mode = WriteMode,
		read_mode = ReadMode,
		connection = Connection,
		database = Database
	}),
	try Action() of
		Result -> Result
	after
		case PrevContext of
			undefined ->
				erlang:erase(mongo_action_context);
			_ ->
				erlang:put(mongo_action_context, PrevContext)
		end
	end.


%% @doc Insert a document or multiple documents into a collection.
%%      Returns the document or documents with an auto-generated _id if missing.
-spec insert(collection(), A) -> A.
insert(Coll, Doc) when is_tuple(Doc) ->
	hd(insert(Coll, [Doc]));
insert(Coll, Docs) ->
	Docs1 = [assign_id(Doc) || Doc <- Docs],
	write(#insert{collection = Coll, documents = Docs1}),
	Docs1.

%% @doc Replace the document matching criteria entirely with the new Document.
-spec update(collection(), selector(), bson:document()) -> ok.
update(Coll, Selector, Doc) ->
	update(Coll, Selector, Doc, false, false).

%% @doc Replace the document matching criteria entirely with the new Document.
-spec update(collection(), selector(), bson:document(), boolean()) -> ok.
update(Coll, Selector, Doc, Upsert) ->
	update(Coll, Selector, Doc, Upsert, false).

%% @doc Replace the document matching criteria entirely with the new Document.
-spec update(collection(), selector(), bson:document(), boolean(), boolean()) -> ok.
update(Coll, Selector, Doc, Upsert, MultiUpdate) ->
	write(#update{collection = Coll, selector = Selector, updater = Doc, upsert = Upsert, multiupdate = MultiUpdate}).

%% @doc Delete selected documents
-spec delete (collection(), selector()) -> ok.
delete(Coll, Selector) ->
	write(#delete{collection = Coll, singleremove = false, selector = Selector}).

%% @doc Delete first selected document.
-spec delete_one (collection(), selector()) -> ok.
delete_one(Coll, Selector) ->
	write(#delete{collection = Coll, singleremove = true, selector = Selector}).

%% @doc Return first selected document, if any
-spec find_one(collection(), selector()) -> {} | {bson:document()}.
find_one(Coll, Selector) ->
	find_one(Coll, Selector, []).

%% @doc Return projection of first selected document, if any. Empty projection [] means full projection.
-spec find_one(collection(), selector(), projector()) -> {} | {bson:document()}.
find_one(Coll, Selector, Projector) ->
	find_one(Coll, Selector, Projector, 0).

%% @doc Return projection of Nth selected document, if any. Empty projection [] means full projection.
-spec find_one (collection(), selector(), projector(), skip()) -> {} | {bson:document()}.
find_one(Coll, Selector, Projector, Skip) ->
	read_one(#'query'{
		collection = Coll,
		selector = Selector,
		projector = Projector,
		skip = Skip
	}).

%% @doc Return selected documents.
-spec find (collection(), selector()) -> cursor().
find(Coll, Selector) ->
	find(Coll, Selector, []).

%% @doc Return projection of selected documents.
%%      Empty projection [] means full projection.
-spec find (collection(), selector(), projector()) -> cursor().
find(Coll, Selector, Projector) ->
	find(Coll, Selector, Projector, 0).

%% @doc Return projection of selected documents starting from Nth document.
%%      Empty projection means full projection.
-spec find (collection(), selector(), projector(), skip()) -> cursor().
find(Coll, Selector, Projector, Skip) ->
	find(Coll, Selector, Projector, Skip, 0).

%% @doc Return projection of selected documents starting from Nth document in batches of batchsize.
%%      0 batchsize means default batch size.
%%      Negative batch size means one batch only.
%%      Empty projection means full projection.
-spec find (collection(), selector(), projector(), skip(), batchsize()) -> cursor(). % Action
find(Coll, Selector, Projector, Skip, BatchSize) ->
	read(#'query'{
		collection = Coll,
		selector = Selector,
		projector = Projector,
		skip = Skip,
		batchsize = BatchSize
	}).

%@doc Count selected documents
-spec count (collection(), selector()) -> integer().
count(Coll, Selector) ->
	count(Coll, Selector, 0).

%@doc Count selected documents up to given max number; 0 means no max.
%     Ie. stops counting when max is reached to save processing time.
-spec count(collection(), selector(), integer()) -> integer().
count(Coll, Selector, Limit) ->
	CollStr = atom_to_binary(Coll, utf8),
	Doc = command(case Limit =< 0 of
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
-spec ensure_index (collection(), bson:document()) -> ok.
ensure_index(Coll, IndexSpec) ->
	#context{database = Database} = erlang:get(mongo_action_context),
	Key = bson:at(key, IndexSpec),
	Defaults = {name, gen_index_name(Key), unique, false, dropDups, false},
	Index = bson:update(ns, mongo_protocol:dbcoll(Database, Coll), bson:merge(IndexSpec, Defaults)),
	insert('system.indexes', Index),
	ok.

%% @doc Execute given MongoDB command and return its result.
-spec command (bson:document()) -> bson:document(). % Action
command(Command) ->
	{Doc} = read_one(#'query'{
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

%% @private
write(Request) ->
	Context = erlang:get(mongo_action_context),
	case Context#context.write_mode of
		unsafe ->
			write(Context#context.connection, Context#context.database, Request);
		SafeMode ->
			Params = case SafeMode of safe -> {}; {safe, Param} -> Param end,
			Ack = write(Context#context.connection, Context#context.database, Request, Params),
			case bson:lookup(err, Ack, undefined) of
				undefined -> ok;
				String ->
					case bson:at(code, Ack) of
						10058 -> erlang:exit(not_master);
						Code -> erlang:exit({write_failure, Code, String})
					end
			end
	end.

%% @private
write(Connection, Database, Request) ->
	mongo_connection:request(Connection, Database, Request).

%% @private
write(Connection, Database, Request, GetLastErrorParams) ->
	ok = mongo_connection:request(Connection, Database, Request),
	{0, [Doc | _]} = mongo_connection:request(Connection, Database, #'query'{
		batchsize = -1,
		collection = '$cmd',
		selector = bson:append({getlasterror, 1}, GetLastErrorParams)
	}),
	Doc.

%% @private
read(Request) ->
	#context{connection = Connection, database = Database, read_mode = ReadMode} = erlang:get(mongo_action_context),
	#'query'{collection = Collection, batchsize = BatchSize} = Request,
	{Cursor, Batch} = mongo_connection:request(Connection, Database, Request#'query'{slaveok = ReadMode =:= slave_ok}),
	mongo_cursor:create(Connection, Database, Collection, Cursor, BatchSize, Batch).

%% @private
read_one(Request) ->
	#context{connection = Connection, database = Database, read_mode = ReadMode} = erlang:get(mongo_action_context),
	{0, Docs} = mongo_connection:request(Connection, Database, Request#'query'{batchsize = -1, slaveok = ReadMode =:= slave_ok}),
	case Docs of
		[] -> {};
		[Doc | _] -> {Doc}
	end.

