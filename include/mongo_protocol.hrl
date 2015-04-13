% Wire protocol message types (records)

-define(GS2_HEADER, <<"n,,">>).

-type db() :: atom().
-type collection() :: binary() | atom(). % without db prefix
-type cursorid() :: integer().
-type selector() :: bson:document().
-type projector() :: bson:document().
-type skip() :: integer().
-type batchsize() :: integer(). % 0 = default batch size. negative closes cursor
-type modifier() :: bson:document().
-type connection() :: pid().
-type database() :: binary() | atom().
-type write_mode() :: unsafe | safe | {safe, bson:document()}.
-type read_mode() :: master | slave_ok.
-type action(A) :: fun (() -> A).
-type service() :: {Host :: inet:hostname() | inet:ip_address(), Post :: 0..65535}.
-type options() :: [option()].
-type option() :: {timeout, timeout()} | {ssl, boolean()} | ssl | {database, database()} | {read_mode, read_mode()} | {write_mode, write_mode()}.

-export_type([connection/0, service/0, options/0]).

%% write
-record(insert, {
	collection :: collection(),
	documents :: [bson:document()]
}).

-record(update, {
	collection :: collection(),
	upsert = false :: boolean(),
	multiupdate = false :: boolean(),
	selector :: selector(),
	updater :: bson:document() | modifier()
}).

-record(delete, {
	collection :: collection(),
	singleremove = false :: boolean(),
	selector :: selector()
}).

%% read
-record('query', {
	tailablecursor = false :: boolean(),
	slaveok = false :: boolean(),
	nocursortimeout = false :: boolean(),
	awaitdata = false :: boolean(),
	collection :: collection(),
	skip = 0 :: skip(),
	batchsize = 0 :: batchsize(),
	selector :: selector(),
	projector = [] :: projector()
}).

-record(getmore, {
	collection :: collection(),
	batchsize = 0 :: batchsize(),
	cursorid :: cursorid()
}).

%% system
-record(ensure_index, {
	collection :: collection(),
	index_spec
}).

-record(conn_state, {
	write_mode = unsafe :: write_mode(),
	read_mode = master :: read_mode(),
	database :: database()
}).

-record(killcursor, {
	cursorids :: [cursorid()]
}).

-record(reply, {
	cursornotfound :: boolean(),
	queryerror :: boolean(),
	awaitcapable :: boolean(),
	cursorid :: cursorid(),
	startingfrom :: integer(),
	documents :: [bson:document()]
}).