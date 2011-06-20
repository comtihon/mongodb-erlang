%@doc Top-level client interface to MongoDB
-module (mongo).

-export_type ([maybe/1]).

-export_type ([host/0, connection/0]).
-export ([connect/1, connect/2, disconnect/1, connect_factory/1, connect_factory/2]).
-export_type ([replset/0, rs_connection/0]).
-export ([rs_connect/1, rs_connect/2, rs_disconnect/1, rs_connect_factory/1, rs_connect_factory/2]).

-export_type ([action/1, db/0, write_mode/0, read_mode/0, failure/0]).
-export ([do/5, this_db/0]).

-export_type ([collection/0, selector/0, projector/0, skip/0, batchsize/0, modifier/0]).
-export ([insert/2, insert_all/2]).
-export ([save/2, replace/3, repsert/3, modify/3]).
-export ([delete/2, delete_one/2]).
-export ([find_one/2, find_one/3, find_one/4]).
-export ([find/2, find/3, find/4, find/5]).
-export ([count/2, count/3]).

-export_type ([cursor/0]).
-export ([next/1, rest/1, close_cursor/1]).

-export_type ([command/0]).
-export ([command/1]).

-export_type ([username/0, password/0]).
-export ([auth/2]).

-export_type ([permission/0]).
-export ([add_user/3]).

-export_type ([index_spec/0, key_order/0]).
-export ([create_index/2]).

-export ([copy_database/3, copy_database/5]).

-include ("mongo_protocol.hrl").

-type reason() :: any().

% Server %

-type host() :: mongo_connect:host().
% Hostname or ip address with or without port. Port defaults to 27017 when missing.
% Eg. "localhost" or {"localhost", 27017}
-type connection() :: mongo_connect:connection().

-spec connect (host()) -> {ok, connection()} | {error, reason()}. % IO
%@doc Connect to given MongoDB server
connect (Host) -> mongo_connect:connect (Host).

-spec connect (host(), timeout()) -> {ok, connection()} | {error, reason()}. % IO
%@doc Connect to given MongoDB server. Timeout used for initial connection and every query and safe write.
connect (Host, TimeoutMS) -> mongo_connect:connect (Host, TimeoutMS).

-spec disconnect (connection()) -> ok. % IO
%@doc Close connection to server
disconnect (Conn) -> mongo_connect:close (Conn).

-spec connect_factory (host()) -> resource_pool:factory(connection()).
%@doc Factory for use with a connection pool. See resource_pool module.
connect_factory (Host) -> connect_factory (Host, infinity).

-spec connect_factory (host(), timeout()) -> resource_pool:factory(connection()).
%@doc Factory for use with a connection pool. See resource_pool module.
connect_factory (Host, TimeoutMS) -> {Host, fun (H) -> connect (H, TimeoutMS) end, fun disconnect/1, fun mongo_connect:is_closed/1}.

% Replica Set %

-type replset() :: mongo_replset:replset().
-type rs_connection() :: mongo_replset:rs_connection().

-spec rs_connect (replset()) -> rs_connection(). % IO
%@doc Create new cache of connections to replica set members starting with seed members. No connection attempted until rs_primary or rs_secondary_ok called.
rs_connect (Replset) -> mongo_replset:connect (Replset).

-spec rs_connect (replset(), timeout()) -> rs_connection(). % IO
%@doc Create new cache of connections to replica set members starting with seed members. No connection attempted until rs_primary or rs_secondary_ok called. Timeout used for initial connection and every query and safe write.
rs_connect (Replset, TimeoutMS) -> mongo_replset:connect (Replset, TimeoutMS).

-spec rs_disconnect (rs_connection()) -> ok. % IO
%@doc Close cache of replset connections
rs_disconnect (ReplsetConn) -> mongo_replset:close (ReplsetConn).

-spec rs_connect_factory (replset()) -> resource_pool:factory(rs_connection()).
%@doc Factory for use with a rs_connection pool. See resource_pool module.
rs_connect_factory (ReplSet) -> rs_connect_factory (ReplSet, infinity).

-spec rs_connect_factory (replset(), timeout()) -> resource_pool:factory(rs_connection()).
%@doc Factory for use with a rs_connection pool. See resource_pool module.
rs_connect_factory (Replset, TimeoutMS) -> {Replset, fun (RS) -> RC = rs_connect (RS, TimeoutMS), {ok, RC} end, fun rs_disconnect/1, fun mongo_replset:is_closed/1}.

% Action %

-type action(A) :: fun (() -> A).
% An Action does IO, reads process dict {mongo_action_context, #context{}}, and throws failure()

-type failure() ::
	mongo_connect:failure() |  % thrown by read and safe write
	mongo_query:not_master() |  % thrown by read and safe write
	mongo_query:unauthorized() |  % thrown by read and safe write
	write_failure() |  % thrown by safe write
	mongo_cursor:expired().  % thrown by cursor next/rest

-record (context, {
	write_mode :: write_mode(),
	read_mode :: read_mode(),
	dbconn :: mongo_connect:dbconnection() }).

-spec do (write_mode(), read_mode(), connection() | rs_connection(), db(), action(A)) -> {ok, A} | {failure, failure()}. % IO
%@doc Execute mongo action under given write_mode, read_mode, connection, and db. Return action result or failure.
do (WriteMode, ReadMode, Connection, Database, Action) -> case connection_mode (ReadMode, Connection) of
	{error, Reason} -> {failure, {connection_failure, Reason}};
	{ok, Conn} ->
		PrevContext = get (mongo_action_context),
		put (mongo_action_context, #context {write_mode = WriteMode, read_mode = ReadMode, dbconn = {Database, Conn}}),
		try Action() of
			Result -> {ok, Result}
		catch
			throw: E = {connection_failure, _, _} -> {failure, E};
			throw: E = not_master -> {failure, E};
			throw: E = unauthorized -> {failure, E};
			throw: E = {write_failure, _, _} -> {failure, E};
			throw: E = {cursor_expired, _} -> {failure, E}
		after
			case PrevContext of undefined -> erase (mongo_action_context); _ -> put (mongo_action_context, PrevContext) end
		end end.

-spec connection_mode (read_mode(), connection() | rs_connection()) -> {ok, connection()} | {error, reason()}. % IO
%@doc For rs_connection return appropriate primary or secondary connection
connection_mode (_, Conn = {connection, _, _, _}) -> {ok, Conn};
connection_mode (master, RsConn = {rs_connection, _, _, _}) -> mongo_replset:primary (RsConn);
connection_mode (slave_ok, RsConn = {rs_connection, _, _, _}) -> mongo_replset:secondary_ok (RsConn).

-spec this_db () -> db(). % Action
%@doc Current db in context that we are querying
this_db () -> {Db, _} = (get (mongo_action_context)) #context.dbconn, Db.

% Write %

-type write_mode() :: unsafe | safe | {safe, mongo_query:getlasterror_request()}.
% Every write inside an action() will use this write mode.
% unsafe = asynchronous write (no reply) and hence may silently fail;
% safe = synchronous write, wait for reply and fail if connection or write failure;
% {safe, Params} = same as safe but with extra params for getlasterror, see its documentation at http://www.mongodb.org/display/DOCS/getLastError+Command.

-type write_failure() :: {write_failure, error_code(), bson:utf8()}.
-type error_code() :: integer().

-spec write (mongo_query:write()) -> ok. % Action
%@doc Do unsafe unacknowledged fast write or safe acknowledged slower write depending on our context. When safe, throw write_failure if acknowledgment (getlasterror) reports error.
write (Write) ->
	Context = get (mongo_action_context),
	case Context #context.write_mode of
		unsafe -> mongo_query:write (Context #context.dbconn, Write);
		SafeMode -> 
			Params = case SafeMode of safe -> {}; {safe, Param} -> Param end,
			Ack = mongo_query:write (Context #context.dbconn, Write, Params),
			case bson:lookup (err, Ack) of
				{} -> ok; {null} -> ok;
				{String} -> case bson:at (code, Ack) of
					10058 -> throw (not_master);
					Code -> throw ({write_failure, Code, String}) end end end.

-spec insert (collection(), bson:document()) -> bson:value(). % Action
%@doc Insert document into collection. Return its '_id' value, which is auto-generated if missing.
insert (Coll, Doc) -> [Value] = insert_all (Coll, [Doc]), Value.

-spec insert_all (collection(), [bson:document()]) -> [bson:value()]. % Action
%@doc Insert documents into collection. Return their '_id' values, which are auto-generated if missing.
insert_all (Coll, Docs) ->
	Docs1 = lists:map (fun assign_id/1, Docs),
	write (#insert {collection = Coll, documents = Docs1}),
	lists:map (fun (Doc) -> bson:at ('_id', Doc) end, Docs1).

-spec assign_id (bson:document()) -> bson:document(). % IO
%@doc If doc has no '_id' field then generate a fresh object id for it
assign_id (Doc) -> case bson:lookup ('_id', Doc) of
	{_Value} -> Doc;
	{} -> bson:append ({'_id', mongodb_app:gen_objectid()}, Doc) end.

-spec save (collection(), bson:document()) -> ok. % Action
%@doc If document has no '_id' field then insert it, otherwise update it and insert only if missing.
save (Coll, Doc) -> case bson:lookup ('_id', Doc) of
	{} -> insert (Coll, Doc), ok;
	{Id} -> repsert (Coll, {'_id', Id}, Doc) end.

-spec replace (collection(), selector(), bson:document()) -> ok. % Action
%@doc Replace first document selected with given document.
replace (Coll, Selector, Doc) -> update (false, false, Coll, Selector, Doc).

-spec repsert (collection(), selector(), bson:document()) -> ok. % Action
%@doc Replace first document selected with given document, or insert it if selection is empty.
repsert (Coll, Selector, Doc) -> update (true, false, Coll, Selector, Doc).

-spec modify (collection(), selector(), modifier()) -> ok. % Action
%@doc Update all documents selected using modifier
modify (Coll, Selector, Mod) -> update (false, true, Coll, Selector, Mod).

-spec update (boolean(), boolean(), collection(), selector(), bson:document()) -> ok. % Action
update (Upsert, MultiUpdate, Coll, Sel, Doc) ->
	write (#update {collection = Coll, upsert = Upsert, multiupdate = MultiUpdate, selector = Sel, updater = Doc}).

-spec delete (collection(), selector()) -> ok. % Action
%@doc Delete selected documents
delete (Coll, Selector) ->
	write (#delete {collection = Coll, singleremove = false, selector = Selector}).

-spec delete_one (collection(), selector()) -> ok. % Action
%@doc Delete first selected document.
delete_one (Coll, Selector) ->
	write (#delete {collection = Coll, singleremove = true, selector = Selector}).

% Read %

-type read_mode() :: master | slave_ok.
% Every query inside an action() will use this mode.
% master = Server must be master/primary so reads are consistent (read latest writes).
% slave_ok = Server may be slave/secondary so reads may not be consistent (may read stale data). Slaves will eventually get the latest writes, so technically this is called eventually-consistent.

slave_ok (#context {read_mode = slave_ok}) -> true;
slave_ok (#context {read_mode = master}) -> false.

-type maybe(A) :: {A} | {}.

-spec find_one (collection(), selector()) -> maybe (bson:document()). % Action
%@doc Return first selected document, if any
find_one (Coll, Selector) -> find_one (Coll, Selector, []).

-spec find_one (collection(), selector(), projector()) -> maybe (bson:document()). % Action
%@doc Return projection of first selected document, if any. Empty projection [] means full projection.
find_one (Coll, Selector, Projector) -> find_one (Coll, Selector, Projector, 0).

-spec find_one (collection(), selector(), projector(), skip()) -> maybe (bson:document()). % Action
%@doc Return projection of Nth selected document, if any. Empty projection [] means full projection.
find_one (Coll, Selector, Projector, Skip) ->
	Context = get (mongo_action_context),
	Query = #'query' {
		collection = Coll, selector = Selector, projector = Projector,
		skip = Skip, slaveok = slave_ok (Context) },
	mongo_query:find_one (Context #context.dbconn, Query).

-spec find (collection(), selector()) -> cursor(). % Action
%@doc Return selected documents.
find (Coll, Selector) -> find (Coll, Selector, []).

-spec find (collection(), selector(), projector()) -> cursor(). % Action
%@doc Return projection of selected documents. Empty projection [] means full projection.
find (Coll, Selector, Projector) -> find (Coll, Selector, Projector, 0).

-spec find (collection(), selector(), projector(), skip()) -> cursor(). % Action
%@doc Return projection of selected documents starting from Nth document. Empty projection means full projection.
find (Coll, Selector, Projector, Skip) -> find (Coll, Selector, Projector, Skip, 0).

-spec find (collection(), selector(), projector(), skip(), batchsize()) -> cursor(). % Action
%@doc Return projection of selected documents starting from Nth document in batches of batchsize. 0 batchsize means default batch size. Negative batch size means one batch only. Empty projection means full projection.
find (Coll, Selector, Projector, Skip, BatchSize) ->
	Context = get (mongo_action_context),
	Query = #'query' {
		collection = Coll, selector = Selector, projector = Projector,
		skip = Skip, batchsize = BatchSize, slaveok = slave_ok (Context) },
	mongo_query:find (Context #context.dbconn, Query).

-type cursor() :: mongo_cursor:cursor().

-spec next (cursor()) -> maybe (bson:document()). % IO throws mongo_connect:failure() & mongo_cursor:expired() (this is a subtype of Action)
%@doc Return next document in query result cursor, if any.
next (Cursor) -> mongo_cursor:next (Cursor).

-spec rest (cursor()) -> [bson:document()]. % IO throws mongo_connect:failure() & mongo_cursor:expired() (this is a subtype of Action)
%@doc Return remaining documents in query result cursor.
rest (Cursor) -> mongo_cursor:rest (Cursor).

-spec close_cursor (cursor()) -> ok. % IO (IO is a subtype of Action)
%@doc Close cursor
close_cursor (Cursor) -> mongo_cursor:close (Cursor).

-spec count (collection(), selector()) -> integer(). % Action
%@doc Count selected documents
count (Coll, Selector) -> count (Coll, Selector, 0).

-spec count (collection(), selector(), integer()) -> integer(). % Action
%@doc Count selected documents up to given max number; 0 means no max. Ie. stops counting when max is reached to save processing time.
count (Coll, Selector, Limit) ->
	CollStr = atom_to_binary (Coll, utf8),
	Command = if
		Limit =< 0 -> {count, CollStr, 'query', Selector};
		true -> {count, CollStr, 'query', Selector, limit, Limit} end,
	Doc = command (Command),
	trunc (bson:at (n, Doc)). % Server returns count as float

% Command %

-type command() :: mongo_query:command().

-spec command (command()) -> bson:document(). % Action
%@doc Execute given MongoDB command and return its result.
command (Command) ->
	Context = get (mongo_action_context),
	mongo_query:command (Context #context.dbconn, Command, slave_ok (Context)).

% Authentication %

-type username() :: bson:utf8().
-type password() :: bson:utf8().
-type nonce() :: bson:utf8().

-spec auth (username(), password()) -> boolean(). % Action
%@doc Authenticate with the database (if server is running in secure mode). Return whether authentication was successful or not. Reauthentication is required for every new pipe.
auth (Username, Password) ->
	Nonce = bson:at (nonce, command ({getnonce, 1})),
	try command ({authenticate, 1, user, Username, nonce, Nonce, key, pw_key (Nonce, Username, Password)})
		of _ -> true
		catch error:{bad_command, _} -> false end.

-spec pw_key (nonce(), username(), password()) -> bson:utf8().
pw_key (Nonce, Username, Password) -> bson:utf8 (binary_to_hexstr (crypto:md5 ([Nonce, Username, pw_hash (Username, Password)]))).

-spec pw_hash (username(), password()) -> bson:utf8().
pw_hash (Username, Password) -> bson:utf8 (binary_to_hexstr (crypto:md5 ([Username, <<":mongo:">>, Password]))).

-spec binary_to_hexstr (binary()) -> string().
binary_to_hexstr (Bin) ->
	lists:flatten ([io_lib:format ("~2.16.0b", [X]) || X <- binary_to_list (Bin)]).

-type permission() :: read_write | read_only.

-spec add_user (permission(), username(), password()) -> ok. % Action
%@doc Add user with given access rights (permission)
add_user (Permission, Username, Password) ->
	User = case find_one (system.users, {user, Username}) of {} -> {user, Username}; {Doc} -> Doc end,
	Rec = {readOnly, case Permission of read_only -> true; read_write -> false end, pwd, pw_hash (Username, Password)},
	save (system.users, bson:merge (Rec, User)).

% Index %

-type index_spec() :: bson:document().
% The following fields are required:
%	key : key_order()
% The following fields are optional:
%	name : bson:utf8()
%	unique : boolean()
%	dropDups : boolean()
% Additional fields are allowed specific to the index, for example, when creating a Geo index you may also supply
% min & max fields. See http://www.mongodb.org/display/DOCS/Geospatial+Indexing for details.

-type key_order() :: bson:document().
% Fields to index on and whether ascending (1) or descending (-1) or Geo (<<"2d">>). Eg. {x,1, y,-1} or {loc, <<"2d">>}

-spec create_index (collection(), index_spec() | key_order()) -> ok. % Action
%@doc Create index on collection according to given spec. Allow user to just supply key
create_index (Coll, IndexSpec) ->
	Db = this_db (),
	Index = bson:append ({ns, mongo_protocol:dbcoll (Db, Coll)}, fillout_indexspec (IndexSpec)),
	insert ('system.indexes', Index).

-spec fillout_indexspec (index_spec() | key_order()) -> index_spec().
% Fill in missing optonal fields with defaults. Allow user to just supply key_order
fillout_indexspec (IndexSpec) -> case bson:lookup (key, IndexSpec) of
	{Key} when is_tuple (Key) -> bson:merge (IndexSpec, {key, Key, name, gen_index_name (Key), unique, false, dropDups, false});
	{_} -> {key, IndexSpec, name, gen_index_name (IndexSpec), unique, false, dropDups, false}; % 'key' happens to be a user field
	{} -> {key, IndexSpec, name, gen_index_name (IndexSpec), unique, false, dropDups, false} end.

-spec gen_index_name (key_order()) -> bson:utf8().
gen_index_name (KeyOrder) ->
	AsName = fun (Label, Order, Name) -> <<
		Name /binary, $_,
		(atom_to_binary (Label, utf8)) /binary, $_,
		(if
			is_integer (Order) -> bson:utf8 (integer_to_list (Order));
			is_atom (Order) -> atom_to_binary (Order, utf8);
			is_binary (Order) -> Order;
			true -> <<>> end) /binary >> end,
	bson:doc_foldl (AsName, <<"i">>, KeyOrder).

% Admin

-spec copy_database (db(), host(), db()) -> bson:document(). % Action
% Copy database from given host to the server I am connected to. Must be connected to 'admin' database.
copy_database (FromDb, FromHost, ToDb) ->
	command ({copydb, 1, fromhost, mongo_connect:show_host (FromHost), fromdb, atom_to_binary (FromDb, utf8), todb, atom_to_binary (ToDb, utf8)}).

-spec copy_database (db(), host(), db(), username(), password()) -> bson:document(). % Action
% Copy database from given host, authenticating with given username and password, to the server I am connected to. Must be connected to 'admin' database.
copy_database (FromDb, FromHost, ToDb, Username, Password) ->
	Nonce = bson:at (nonce, command ({copydbgetnonce, 1, fromhost, mongo_connect:show_host (FromHost)})),
	command ({copydb, 1, fromhost, mongo_connect:show_host (FromHost), fromdb, atom_to_binary (FromDb, utf8), todb, atom_to_binary (ToDb, utf8), username, Username, nonce, Nonce, key, pw_key (Nonce, Username, Password)}).
