%%%-------------------------------------------------------------------
%%% @author Alexander Hudich (alttagil@gmail.com)
%%%-------------------------------------------------------------------
-author("alttagil@gmail.com").


-record( mc_server, {
	pid,
	mref,

	host,

	type = unknown,

	me,

	old_rtt = undefined,
	rtt = undefined,

	setName,
	setVersion,

	tags=[],

	minWireVersion,
	maxWireVersion,

	hosts=[],
	passives=[],
	arbiters=[],

	electionId,

	primary,
	ismaster,
	secondary
} ).