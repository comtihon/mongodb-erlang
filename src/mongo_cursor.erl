% A cursor references pending query results on a server.
% Implemented as a process that dies if idle for more than 10 minutes. The referenced cursor on the server also dies after 10 minutes of idle.
-module (mongo_cursor).

-export_type ([cursor/0, cursorexpired/0]).

-export ([next/1, rest/1, close/1]).

-export ([cursor/4]). % for mongo_query only

-include ("mongo_protocol.hrl").

-opaque cursor() :: {cursor,
	{mongo_connect:dbconnection(), collection(), batchsize()},
	var:mvar ({cursorid(), [bson:document()]}) }.

-type batchsize() :: integer().

% RIO means does IO and may throw mongo_connect:connectionfailure() or cursorexpired().

-type cursorexpired() :: {cursorexpired, cursorid()}.

-spec cursor (mongo_connect:dbconnection(), collection(), batchsize(), {cursorid(), [bson:document()]}) -> cursor. % IO
% Create new cursor from result batch
cursor (DbConn, Collection, BatchSize, Batch) ->
	{cursor, DbConn, Collection, BatchSize, var:new (mongodb_app, 10 * 60 * 1000, Batch)}.

-spec next (cursor()) -> maybe:maybe (bson:document()). % RIO
% Return next document in query result, or nothing if finished. Throw cursorexpired if cursor was idle for too long, hence server deleted it. Throw connectionfailure if network problem.
next ({cursor, Env, Var}) -> var:modify (Var, fun (Batch) -> xnext (Env, Batch) end).

xnext (Env = {DbConn, Coll, BatchSize}, {CursorId, Docs}) -> case Docs of
	[Doc | Docs1] -> {{CursorId, Docs1}, {just, Doc}};
	[] -> case CursorId of
		0 -> {{0, []}, nothing};
		_ ->
			Getmore = #getmore {collection = Coll, batchsize = BatchSize, cursorid = CursorId},
			Reply = mongo_connect:call (DbConn, [], Getmore),
			xnext (Env, batch_reply (Reply)) end end.

-spec batch_reply (mongo_protocol:reply()) -> {cursorid(), [bson:document()]}. % RIO
% Extract next batch of results from reply. Throw cursorexpired if cursor not found on server (expired).
batch_reply (#reply {
	cursornotfound = CursorNotFound, queryerror = false, awaitcapable = _,
	cursorid = CursorId, startingfrom = _, documents = Docs }) -> if
		CursorNotFound -> throw ({cursorexpired, CursorId});
		true -> {CursorId, Docs} end.

-spec rest (cursor()) -> [bson:document()]. % RIO
% Return remaining documents in query result
rest (Cursor) -> case next (Cursor) of
	nothing -> [];
	{just, Doc} -> [Doc | rest (Cursor)] end.

-spec close (cursor()) -> ok. % CIO
close ({cursor, {DbConn, _, _}, Var}) ->
	case var:kill (Var) of
		nothing -> ok;  % already closed
		{just, {CursorId, _}} -> case CursorId of
			0 -> ok;
			_ -> mongo_connect:send (DbConn, [#killcursor {cursorids = [CursorId]}]) end end.
