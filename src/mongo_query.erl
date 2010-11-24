% write, find, and command operations
-module (mongo_query).

-export_type ([write/0, 'query'/0, command/0]).
-export_type ([getlasterror_request/0, getlasterror_reply/0]).
-export_type ([query_error/0]).

-export ([write/3, write/2, find_one/2, find/2, command/3]).

-include ("mongo_protocol.hrl").

% CIO means does IO and may throw mongo_connect:failure().

-type query_error() :: {query_error, bson:utf8()}.

-type write() :: #insert{} | #update{} | #delete{}.

-type getlasterror_request() :: bson:document().
% Parameters to getlasterror request.
-type getlasterror_reply() :: bson:document().
% Reply to getlasterror request.

-spec write (mongo_connect:dbconnection(), write(), getlasterror_request()) -> getlasterror_reply(). % CIO
% Send write and getlasterror request to mongodb over connection and wait for and return getlasterror reply. Bad getlasterror params are ignored.
% Caller is responsible for checking for error in reply; if 'err' field is null then success, otherwise it holds error message string.
write (DbConn, Write, GetLastErrorParams) ->
	Query = #'query' {batchsize = -1, collection = '$cmd', selector = [getlasterror, 1 | GetLastErrorParams]},
	Reply = mongo_connect:call (DbConn, [Write], Query),
	{0, [Doc | _]} = query_reply (Reply),
	Doc.

-spec write (mongo_connect:dbconnection(), write()) -> ok. % IO
% Send write to mongodb over connection asynchronously. Does not wait for reply hence may silently fail. Doesn't even throw connection failure if connection is closed.
write (DbConn, Write) ->
	mongo_connect:send (DbConn, [Write]).

-type command() :: bson:document().

-spec command (mongo_connect:dbconnection(), command(), boolean()) -> bson:document(). % CIO
% Send command to mongodb over connection and wait for reply and return it. Boolean arg indicates slave-ok or not. 'bad_command' error if bad command.
command (DbConn, Command, SlaveOk) ->
	Query = #'query' {collection = '$cmd', selector = Command, slaveok = SlaveOk},
	{Doc} = find_one (DbConn, Query),
	Ok = bson:at (ok, Doc),
	if Ok == true orelse Ok == 1 -> Doc; true -> erlang:error ({bad_command, Doc}) end.

-type 'query'() :: #'query'{}.

-type maybe(A) :: {A} | {}.

-spec find_one (mongo_connect:dbconnection(), 'query'()) -> maybe (bson:document()). % CIO
% Send read request to mongodb over connection and wait for reply. Return first result or nothing if empty.
find_one (DbConn, Query) ->
	Query1 = Query #'query' {batchsize = -1},
	Reply = mongo_connect:call (DbConn, [], Query1),
	{0, Docs} = query_reply (Reply),  % batchsize negative so cursor should be closed (0)
	case Docs of [] -> {}; [Doc | _] -> {Doc} end.

-spec find (mongo_connect:dbconnection(), 'query'()) -> mongo_cursor:cursor(). % CIO
% Send read request to mongodb over connection and wait for reply of first batch. Return a cursor holding this batch and able to fetch next batch on demand.
find (DbConn, Query) ->
	Reply = mongo_connect:call (DbConn, [], Query),
	mongo_cursor:cursor (DbConn, Query #'query'.collection, Query #'query'.batchsize, query_reply (Reply)).

-spec query_reply (mongo_protocol:reply()) -> {cursorid(), [bson:document()]}. % CIO
% Extract cursorid and results from reply. 'bad_query' error if query error.
query_reply (#reply {
	cursornotfound = false, queryerror = QueryError, awaitcapable = _,
	cursorid = Cid, startingfrom = _, documents = Docs }) ->
		Error = case {QueryError, Docs} of
			{true, [_ | _]} -> true;
			{false, [Doc | _]} -> case bson:lookup ('$err', Doc) of
				{_} -> true; % first doc error doc (Server should also set QueryError)
				{} -> false end;
			_ -> false end,
		case Error of
			true -> erlang:error ({bad_query, hd (Docs)});
			false -> {Cid, Docs} end.
