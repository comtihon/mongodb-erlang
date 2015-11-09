% Wire protocol message types (records)

-define(GS2_HEADER, <<"n,,">>).

-type colldb() :: collection() | { database(), collection() }.
-type collection() :: binary() | atom(). % without db prefix
-type database() :: binary() | atom().


%% write
-record(insert, {
  collection :: colldb(),
  documents :: [bson:document()]
}).

-record(update, {
  collection :: colldb(),
  upsert = false :: boolean(),
  multiupdate = false :: boolean(),
  selector :: mongo:selector(),
  updater :: bson:document() | mongo:modifier()
}).

-record(delete, {
  collection :: colldb(),
  singleremove = false :: boolean(),
  selector :: mongo:selector()
}).

%% read
-record('query', {
  collection :: colldb(),
  tailablecursor = false :: boolean(),
  slaveok = false :: boolean(),
  sok_overriden = false :: boolean(),
  nocursortimeout = false :: boolean(),
  awaitdata = false :: boolean(),
  skip = 0 :: mongo:skip(),
  batchsize = 0 :: mongo:batchsize(),
  selector :: mongo:selector(),
  projector = [] :: mongo:projector()
}).

-record(getmore, {
  collection :: colldb(),
  batchsize = 0 :: mongo:batchsize(),
  cursorid :: mongo:cursorid()
}).

%% system
-record(ensure_index, {
  collection :: colldb(),
  index_spec
}).

-record(conn_state, {
  write_mode = unsafe :: mongo:write_mode(),
  read_mode = master :: mongo:read_mode(),
  database :: mongo:database()
}).

-record(killcursor, {
  cursorids :: [mongo:cursorid()]
}).

-record(reply, {
  cursornotfound :: boolean(),
  queryerror :: boolean(),
  awaitcapable :: boolean(),
  cursorid :: mongo:cursorid(),
  startingfrom :: integer(),
  documents :: [bson:document()]
}).