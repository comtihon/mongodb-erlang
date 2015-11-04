%%%-------------------------------------------------------------------
%%% @author alt
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Окт. 2015 23:18
%%%-------------------------------------------------------------------
-author("alt").


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