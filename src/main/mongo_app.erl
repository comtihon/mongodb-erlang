-module(mongo_app).

-behaviour(application).
-export([
	start/2,
	stop/1
]).


%% @hidden
start(_, _) ->
	mc_super_sup:start_link().

%% @hidden
stop(_) ->
	ok.

