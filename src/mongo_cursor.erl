%@doc A cursor references pending query results on a server.
% TODO: terminate cursor after idle for 10 minutes.
-module (mongo_cursor).

-export_type ([maybe/1]).
-export_type ([cursor/0, expired/0]).

-export ([next/1, rest/1, take/2, close/1, is_closed/1]). % API
-export ([cursor/4]). % for mongo_query

-include ("mongo_protocol.hrl").

-type maybe(A) :: {A} | {}.

-opaque cursor() :: mvar:mvar (state()).
% Thread-safe cursor, ie. access to query results

-type expired() :: {cursor_expired, cursor()}.

-type state() :: {env(), batch()}.
-type env() :: {mongo_connect:dbconnection(), collection(), batchsize()}.
-type batch() :: {cursorid(), [bson:document()]}.

-spec cursor (mongo_connect:dbconnection(), collection(), batchsize(), {cursorid(), [bson:document()]}) -> cursor(). % IO
%@doc Create new cursor from result batch
cursor (DbConn, Collection, BatchSize, Batch) ->
	mvar:new ({{DbConn, Collection, BatchSize}, Batch}, fun finalize/1).

-spec close (cursor()) -> ok. % IO
%@doc Close cursor
close (Cursor) -> mvar:terminate (Cursor).

-spec is_closed (cursor()) -> boolean(). % IO
%@doc Is cursor closed
is_closed (Cursor) -> mvar:is_terminated (Cursor).

-spec rest (cursor()) -> [bson:document()]. % IO throws expired() & mongo_connect:failure()
%@doc Return remaining documents in query result
rest (Cursor) -> case next (Cursor) of
	{} -> [];
	{Doc} -> [Doc | rest (Cursor)] end.

-spec next (cursor()) -> maybe (bson:document()). % IO throws expired() & mongo_connect:failure()
%@doc Return next document in query result or nothing if finished.
next (Cursor) ->
	Next = fun ({Env, Batch}) ->
		{Batch1, MDoc} = xnext (Env, Batch),
		{{Env, Batch1}, MDoc} end,
	try mvar:modify (Cursor, Next)
		of {} -> close (Cursor), {}; {Doc} -> {Doc}
		catch expired -> close (Cursor), throw ({cursor_expired, Cursor}) end.

-spec take (non_neg_integer(), cursor()) -> [bson:document()].
%%@doc Return at most requested number of documents from query result cursor.
take (0, _Cursor) -> [];
take (Limit, Cursor) when Limit > 0 ->
    case mongo:next(Cursor) of
        {}    -> [];
        {Doc} -> [Doc | take (Limit - 1, Cursor)]
    end.

-spec xnext (env(), batch()) -> {batch(), maybe (bson:document())}. % IO throws expired & mongo_connect:failure()
%@doc Get next document in cursor, fetching next batch from server if necessary
xnext (Env = {DbConn, Coll, BatchSize}, {CursorId, Docs}) -> case Docs of
	[Doc | Docs1] -> {{CursorId, Docs1}, {Doc}};
	[] -> case CursorId of
		0 -> {{0, []}, {}};
		_ ->
			Getmore = #getmore {collection = Coll, batchsize = BatchSize, cursorid = CursorId},
			Reply = mongo_connect:call (DbConn, [], Getmore),
			xnext (Env, batch_reply (Reply)) end end.

-spec batch_reply (mongo_protocol:reply()) -> batch(). % IO throws expired
%@doc Extract next batch of results from reply. Throw expired if cursor not found on server.
batch_reply (#reply {
	cursornotfound = CursorNotFound, queryerror = false, awaitcapable = _,
	cursorid = CursorId, startingfrom = _, documents = Docs }) -> if
		CursorNotFound -> throw (expired);
		true -> {CursorId, Docs} end.

-spec finalize (state()) -> ok. % IO. Result ignored
%@doc Kill cursor on server if not already
finalize ({{DbConn, _, _}, {CursorId, _}}) -> case CursorId of
	0 -> ok;
	_ -> mongo_connect:send (DbConn, [#killcursor {cursorids = [CursorId]}]) end.
