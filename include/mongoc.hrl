%%%-------------------------------------------------------------------
%%% @author Alexander Hudich (alttagil@gmail.com)
%%%-------------------------------------------------------------------
-author("alttagil@gmail.com").

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