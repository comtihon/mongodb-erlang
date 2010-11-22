% A cursor references pending query results on a server.
% TODO: terminate cursor after idle for 10 minutes.
-module(mongo_cursor).

-export_type ([cursor/0, expired/0, maybe/1]).

-export ([next/1, rest/1, close/1]). % API
-export ([cursor/4]). % for mongo_query

-behaviour (gen_server).
-export ([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include ("mongo_protocol.hrl").

-type server(_) :: pid() | atom(). % Pid or register process name with parameterized state
-type reason() :: any().

-type state() :: {env(), batch()}.
-type env() :: {mongo_connect:dbconnection(), collection(), batchsize()}.
-type batch() :: {cursorid(), [bson:document()]}.
-type batchsize() :: integer().

-type maybe(A) :: {A} | {}.

-opaque cursor() :: server (state()).
% Thread-safe cursor, ie. access to query results

-type expired() :: {cursor_expired, cursor()}.

-spec cursor (mongo_connect:dbconnection(), collection(), batchsize(), {cursorid(), [bson:document()]}) -> cursor(). % IO
% Create new cursor from result batch
cursor (DbConn, Collection, BatchSize, Batch) ->
	{ok, Cursor} = gen_server:start_link (?MODULE, {{DbConn, Collection, BatchSize}, Batch}, []),
	Cursor.

-spec close (cursor()) -> ok. % IO
% Close cursor.
close (Cursor) -> gen_server:cast (Cursor, stop).

-spec next (cursor()) -> maybe (bson:document()). % IO throws expired() & mongo_connect:failure()
% Return next document in query result or nothing if finished.
next (Cursor) -> case gen_server:call (Cursor, next) of
	{ok, Result} -> Result;
	expired -> throw ({cursor_expired, Cursor});
	F = {connection_failure, _, _} -> throw (F) end.

-spec rest (cursor()) -> [bson:document()]. % IO throws expired() & mongo_connect:failure()
% Return remaining documents in query result
rest (Cursor) -> case next (Cursor) of
	{} -> [];
	{Doc} -> [Doc | rest (Cursor)] end.

% gen_server callbacks %

-spec init (state()) -> {ok, state()}.
init (State) -> {ok, State}.

-type tag() :: any(). % Unique tag

-spec handle_call (next, {pid(), tag()}, gen_tcp:socket()) -> {reply, {ok, maybe (bson:document())}, state()} | {stop, expired, expired, state()} | {stop, connection_failure, mongo_connect:failure(), state()}. % IO
% Get next document in cursor or return stop on expired or connection failure
handle_call (next, _From, {Env, Batch}) ->
	try xnext (Env, Batch) of
		{Batch1, MDoc} -> {reply, {ok, MDoc}, {Env, Batch1}}
	catch
		throw: expired -> {stop, expired, expired, {Env, Batch}};
		throw: E = {connection_failure, _, _} -> {stop, connection_failure, E, {Env, Batch}}
	end.

-spec xnext (env(), batch()) -> {batch(), maybe (bson:document())}. % IO throws expired & mongo_connect:failure()
% Get next document in cursor, fetching next batch from server if necessary
xnext (Env = {DbConn, Coll, BatchSize}, {CursorId, Docs}) -> case Docs of
	[Doc | Docs1] -> {{CursorId, Docs1}, {Doc}};
	[] -> case CursorId of
		0 -> {{0, []}, {}};
		_ ->
			Getmore = #getmore {collection = Coll, batchsize = BatchSize, cursorid = CursorId},
			Reply = mongo_connect:call (DbConn, [], Getmore),
			xnext (Env, batch_reply (Reply)) end end.

-spec batch_reply (mongo_protocol:reply()) -> batch(). % IO throws expired
% Extract next batch of results from reply. Throw expired if cursor not found on server.
batch_reply (#reply {
	cursornotfound = CursorNotFound, queryerror = false, awaitcapable = _,
	cursorid = CursorId, startingfrom = _, documents = Docs }) -> if
		CursorNotFound -> throw (expired);
		true -> {CursorId, Docs} end.

-spec handle_cast (stop, state()) -> {stop, normal, state()}.
% Close cursor
handle_cast (stop, State) -> {stop, normal, State}.

handle_info (_Info, Socket) -> {noreply, Socket}.

-spec terminate (reason(), state()) -> any(). % IO. Result ignored
% Kill cursor on server if not already
terminate (expired, _) -> ok;
terminate (connection_failure, _) -> ok;
terminate (_Reason, {{DbConn, _, _}, {CursorId, _}}) -> case CursorId of
	0 -> ok;
	_ -> mongo_connect:send (DbConn, [#killcursor {cursorids = [CursorId]}]) end.

code_change (_OldVsn, State, _Extra) -> {ok, State}.
