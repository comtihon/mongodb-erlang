% write, find, and command operations
-module (mongo_query).

-export_type ([write/0, read/0, command/0]).
-export_type ([getlasterror_request/0, getlasterror_reply/0]).
-export_type ([queryerror/0]).

-export ([write/3, write/2, findone/2, find/2, command/2]).

-include ("mongo_protocol.hrl").

% CIO means does IO and may throw mongo_connect:connectionfailure()
% QIO means does IO and may throw mongo_connect:connectionfailure() or queryerror().

-type queryerror() :: {queryerror, bson:utf8()}.

-type write() :: #insert{} | #update{} | #delete{}.

-type getlasterror_request() :: bson:document().
% Parameters to getlasterror request.
-type getlasterror_reply() :: bson:document().
% Reply to getlasterror request.

-spec write (mongo_connect:dbconnection(), write(), getlasterror_request()) -> getlasterror_reply(). % QIO
% Send write and getlasterror request to mongodb over connection and wait for and return getlasterror reply. Throw queryerror if bad getlasterror request (TODO, confirm this). Throw connectionfailure if network problem.
% Caller is responsible for checking for error in reply; if 'err' field is null then success, otherwise it holds error message string.
write (DbConn, Write, GetLastErrorParams) ->
	Query = #'query' {batchsize = -1, collection = '$cmd', selector = [getlasterror, 1 | GetLastErrorParams]},
	Reply = mongo_connect:call (DbConn, [Write], Query),
	{0, [Doc, _]} = query_reply (Reply),
	Doc.

-spec write (mongo_connect:dbconnection(), write()) -> ok. % CIO
% Send write to mongodb over connection. Does not wait for reply hence may silently fail. Throw connectionfailure if network problem.
write (DbConn, Write) ->
	mongo_connect:send (DbConn, [Write]).

-type command() :: bson:document().

-spec command (mongo_connect:dbconnection(), command()) -> bson:document(). % QIO
% Send command to mongodb over connection and wait for reply and return it. Throw queryerror if bad command (TODO, confirm this). Throw connectionfailure if network problem.
command (DbConn, Command) ->
	Query = #'query' {batchsize = -1, collection = '$cmd', selector = Command},
	Reply = mongo_connect:call (DbConn, [], Query),
	{0, [Doc, _]} = query_reply (Reply),
	Doc.

-type read() :: #'query'{}.

-spec findone (mongo_connect:dbconnection(), read()) -> maybe:maybe (bson:document()). % QIO
% Send read request to mongodb over connection and wait for reply. Return first result or nothing if empty. Throw queryerror if bad query. Throw connectionfailure if network problem.
findone (DbConn, Query) ->
	Query1 = Query #'query' {batchsize = -1},
	Reply = mongo_connect:call (DbConn, [], Query1),
	{0, Docs} = query_reply (Reply),  % findone (limit=-1) cursor should be closed (0)
	maybe:list_to_maybe (Docs).

-spec find (mongo_connect:dbconnection(), read()) -> mongo_cursor:cursor(). % QIO
% Send read request to mongodb over connection and wait for reply of first batch. Return a cursor holding this batch and able to fetch next batch on demand. Throw queryerror if bad query. Throw connectionfailure if network problem.
find (DbConn, Query) ->
	Reply = mongo_connect:call (DbConn, [], Query),
	mongo_cursor:cursor (DbConn, Query #'query'.collection, Query #'query'.batchsize, query_reply (Reply)).

-spec query_reply (mongo_protocol:reply()) -> {cursorid(), [bson:document()]}. % QIO
% Extract cursorid and results from reply. Throw queryerror if query error.
query_reply (#reply {
	cursornotfound = false, queryerror = QueryError, awaitcapable = _,
	cursorid = Cid, startingfrom = _, documents = Docs }) -> if
		QueryError -> throw ({queryerror, bson:at ("$err", hd (Docs))});
		true -> {Cid, Docs} end.
