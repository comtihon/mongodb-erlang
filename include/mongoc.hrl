%%%-------------------------------------------------------------------
%%% @author Alexander Hudich (alttagil@gmail.com)
%%%-------------------------------------------------------------------
-author("alttagil@gmail.com").

-ifndef(MONGOC).
-define(MONGOC, true).

-include("mongo_types.hrl").

-record(mc_server, {
  pid,
  mref,
  host,
  type = unknown,
  me,
  old_rtt = undefined,
  rtt = undefined,
  setName,
  setVersion,
  tags = [],
  minWireVersion,
  maxWireVersion,
  hosts = [],
  passives = [],
  arbiters = [],
  electionId,
  primary,
  ismaster,
  secondary
}).

-record(topology_state, {
  seeds = [],
  self :: pid(),
  type = unknown,
  maxElectionId = undefined,
  setName = undefined,
  setVersion = undefined,
  rp_mode = primary,
  rp_tags = [],
  servers = [] :: list(),
  serverSelectionTimeoutMS = 30000,
  localThresholdMS = 200,
  worker_opts = [],
  topology_opts = [],
  get_pool_timeout :: integer()
}).

-define(TRANSACTION_TIMEOUT, 5000).

-type readmode() :: primary | secondary | primaryPreferred | secondaryPreferred | nearest.
-type host() :: list().
-type seed() :: host()
| {rs, binary(), [host()]}
| {single, host()}
| {unknown, [host()]}
| {sharded, [host()]}.
-type connectoptions() :: [coption()].
-type coption() :: {name, atom()}
|{register, atom()}
|{pool_size, integer()}
|{max_overflow, integer()}
|{localThresholdMS, integer()}
|{connectTimeoutMS, integer()}
|{socketTimeoutMS, integer()}
|{serverSelectionTimeoutMS, integer()}
|{waitQueueTimeoutMS, integer()}
|{heartbeatFrequencyMS, integer()}
|{minHeartbeatFrequencyMS, integer()}
|{rp_mode, readmode()}
|{rp_tags, list()}.
-type workeroptions() :: [woption()].
-type woption() :: {database, database()}
| {login, binary()}
| {password, binary()}
| {w_mode, mc_worker_api:write_mode()}.
-type readprefs() :: [readpref()].
-type readpref() :: #{rp_mode => readmode()}
|{rp_tags, [tuple()]}.
-type reason() :: atom().

-endif.