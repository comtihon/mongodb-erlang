% Wire protocol message types (records)

-define(GS2_HEADER, <<"n,,">>).

-type colldb() :: collection() | {database(), collection()}.
-type collection() :: binary() | atom(). % without db prefix
-type database() :: binary() | atom().


%% write
-record(insert, {
  collection :: colldb(),
  documents :: [map() | bson:document()]
}).

-record(update, {
  collection :: colldb(),
  upsert = false :: boolean(),
  multiupdate = false :: boolean(),
  selector :: mc_worker_api:selector(),
  updater :: bson:document() | mc_worker_api:modifier()
}).

-record(delete, {
  collection :: colldb(),
  singleremove = false :: boolean(),
  selector :: mc_worker_api:selector()
}).

%% read
-record('query', {
  collection :: colldb(),
  tailablecursor = false :: boolean(),
  slaveok = false :: boolean(),
  sok_overriden = false :: boolean(),
  nocursortimeout = false :: boolean(),
  awaitdata = false :: boolean(),
  skip = 0 :: mc_worker_api:skip(),
  batchsize = 0 :: mc_worker_api:batchsize(),
  selector :: mc_worker_api:selector(),
  projector = #{} :: mc_worker_api:projector()
}).

-record(getmore, {
  collection :: colldb(),
  batchsize = 0 :: mc_worker_api:batchsize(),
  cursorid :: mc_worker_api:cursorid()
}).

%% system
-record(ensure_index, {
  collection :: colldb(),
  index_spec
}).

-record(conn_state, {
  write_mode = unsafe :: mc_worker_api:write_mode(),
  read_mode = master :: mc_worker_api:read_mode(),
  database :: mc_worker_api:database()
}).

-record(killcursor, {
  cursorids :: [mc_worker_api:cursorid()]
}).

-record(reply, {
  cursornotfound :: boolean(),
  queryerror :: boolean(),
  awaitcapable :: boolean(),
  cursorid :: mc_worker_api:cursorid(),
  startingfrom :: integer(),
  documents :: [map()]
}).
