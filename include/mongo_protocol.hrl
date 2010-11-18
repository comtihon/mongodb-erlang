% Wire protocol message types (records)

-type collection() :: atom(). % without db prefix

-type cursorid() :: integer().

-record (insert, {
	collection :: collection(),
	documents :: [bson:document()] }).

-record (update, {
	collection :: collection(),
	upsert = false :: boolean(),
	multiupdate = false :: boolean(),
	selector :: bson:document(),
	updater :: bson:document() }).

-record (delete, {
	collection :: collection(),
	singleremove = false :: boolean(),
	selector :: bson:document() }).

-record (killcursor, {
	cursorids :: [cursorid()] }).

-record ('query', {
	tailablecursor = false :: boolean(),
	slaveok = false :: boolean(),
	nocursortimeout = false :: boolean(),
	awaitdata = false :: boolean(),
	collection :: collection(),
	skip = 0 :: integer(),
	batchsize = 0 :: integer(),
	selector :: bson:document(),
	projetor = [] :: bson:document() }).

-record (getmore, {
	collection :: collection(),
	batchsize :: integer(),
	cursorid :: cursorid() }).

-record (reply, {
	cursornotfound :: boolean(),
	queryerror :: boolean(),
	awaitcapable :: boolean(),
	cursorid :: cursorid(),
	startingfrom :: integer(),
	documents :: [bson:document()] }).
