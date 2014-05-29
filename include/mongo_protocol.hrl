% Wire protocol message types (records)

-type db() :: atom().
-type collection() :: atom(). % without db prefix
-type cursorid() :: integer().
-type selector() :: bson:document().
-type projector() :: bson:document().
-type skip() :: integer().
-type batchsize() :: integer(). % 0 = default batch size. negative closes cursor
-type modifier() :: bson:document().

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
	write_mode :: write_mode(),
	read_mode :: read_mode(),
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