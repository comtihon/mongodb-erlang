% Wire protocol message types (records)

-define(GS2_HEADER, <<"n,,">>).

%% write
-record(insert, {
  collection :: mongo:collection(),
  documents :: [bson:document()]
}).

-record(update, {
  collection :: mongo:collection(),
  upsert = false :: boolean(),
  multiupdate = false :: boolean(),
  selector :: mongo:selector(),
  updater :: bson:document() | mongo:modifier()
}).

-record(delete, {
  collection :: mongo:collection(),
  singleremove = false :: boolean(),
  selector :: mongo:selector()
}).

%% read
-record('query', {
  tailablecursor = false :: boolean(),
  slaveok = false :: boolean(),
  sok_overriden = false :: boolean(),
  nocursortimeout = false :: boolean(),
  awaitdata = false :: boolean(),
  collection :: mongo:collection(),
  skip = 0 :: mongo:skip(),
  batchsize = 0 :: mongo:batchsize(),
  selector :: mongo:selector(),
  projector = [] :: mongo:projector()
}).

-record(getmore, {
  collection :: mongo:collection(),
  batchsize = 0 :: mongo:batchsize(),
  cursorid :: mongo:cursorid()
}).

%% system
-record(ensure_index, {
  collection :: mongo:collection(),
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