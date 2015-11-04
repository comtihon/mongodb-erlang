%%%-------------------------------------------------------------------
%%% @author Alexander Hudich (alttagil@gmail.com)
%%% @copyright (C) 2015, Alexander Hudich
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(mc_monitor).
-author("alttagil@gmail.com").

-behaviour(gen_server).

%% API
-export([start_link/5, do_timeout/2, next_loop/1]).

%% gen_server callbacks
-export([init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3]).


-define(SERVER, ?MODULE).

-record( state, { host, port, topology, server, topology_opts, worker_opts, pool, conn_to, timer } ).

%%%===================================================================
%%% API
%%%===================================================================

start_link( Topology, Server, HostPort, Topts, Wopts ) ->
	gen_server:start_link( ?MODULE, [Topology, Server, HostPort, Topts, Wopts], []).



%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Topology, Server, { Host, Port }, Topts, Wopts]) ->
	ConnectTimeoutMS =  proplists:get_value( connectTimeoutMS, Topts, 20000 ),
	gen_server:cast( self(), loop ),
	{ok, #state{ host=Host, port=Port, topology=Topology, server=Server, topology_opts = Topts, worker_opts = Wopts, conn_to = ConnectTimeoutMS }}.


terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.


%%%===================================================================
%%% Handlers
%%%===================================================================


handle_call(_Request, _From, State) ->
	{reply, ok, State}.



handle_cast( loop, State ) ->
	loop( State ),
	{noreply, State};


handle_cast( { loopn, Pid, TimeOut }, State ) ->

	PausePid = State#state.timer,
	if
		PausePid =/= undefined ->
			PausePid ! stop;

		true ->
			ok
	end,

	Timer = spawn_link( ?MODULE, do_timeout, [Pid, TimeOut] ),
	{noreply, State#state{ timer = Timer }, hibernate };


handle_cast( loopn, State ) ->

	PausePid = State#state.timer,
	if
		PausePid =/= undefined ->
			PausePid ! stop;

		true ->
			ok
	end,

	NState = loop( State ),

	{noreply, NState };


handle_cast(_Request, State) ->
	{noreply, State}.



handle_info(_Info, State) ->
	{noreply, State}.



%%%===================================================================
%%% Internal functions
%%%===================================================================


next_loop( Pid, Timestamp ) ->
	gen_server:cast( Pid, { loopn, Pid, Timestamp } ).

next_loop( Pid ) ->
	gen_server:cast( Pid, loopn ).



loop( #state{ host = Host, port = Port, topology = Topology, server = Server, conn_to=Timeout } = State ) ->

	ConnectArgs = [ {host, Host }, {port, Port}, { timeout, Timeout} ],

	Start = erlang:now(),
	{ok, Conn} = mongo:connect( ConnectArgs ),
	{true, IsMaster} = mongo:command( Conn, { isMaster, 1 } ),
	Finish = erlang:now(),
	mongo:disconnect( Conn ),

	gen_server:cast( Topology, { monitor_ismaster, Server, IsMaster, timer:now_diff( Finish, Start ) } ),

	next_loop( self(), 10 ),
	State#state{ timer = undefined }.



do_timeout( Pid, TO ) when TO > 0 ->
	TO2 = TO*1000,
	receive
		stop ->
			ok;

		run ->
			next_loop( Pid )

	after
		TO2 ->
			next_loop( Pid )
	end;


do_timeout( Pid, _TO ) ->
	next_loop( Pid ).