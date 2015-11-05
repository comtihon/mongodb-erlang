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
-export([start_link/5, do_timeout/2, next_loop/1, update_type/2]).

%% gen_server callbacks
-export([init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3]).


-define(SERVER, ?MODULE).

-record( state, { type, host, port, topology, server, topology_opts, worker_opts, pool, conn_to, timer, counter=0 } ).

%%%===================================================================
%%% API
%%%===================================================================

start_link( Topology, Server, HostPort, Topts, Wopts ) ->
	gen_server:start_link( ?MODULE, [Topology, Server, HostPort, Topts, Wopts], []).



%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Topology, Server, { Host, Port }, Topts, Wopts]) ->

	process_flag(trap_exit, true),

	ConnectTimeoutMS =  proplists:get_value( connectTimeoutMS, Topts, 20000 ),
	gen_server:cast( self(), loop ),
	{ok, #state{ host=Host, port=Port, topology=Topology, server=Server, topology_opts = Topts, worker_opts = Wopts, conn_to = ConnectTimeoutMS }}.


terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.


update_type( Pid, Type ) ->
	gen_server:cast( Pid, { update_type, Type } ).


%%%===================================================================
%%% Handlers
%%%===================================================================


handle_call(_Request, _From, State) ->
	{reply, ok, State}.




handle_cast( { update_type, Type }, State ) ->
	{noreply, State#state{ type = Type }};

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


handle_info({'EXIT', Pid, _Reason}, #state{ server = Pid } = State) ->
	exit( kill ),
	{noreply, State };


handle_info(_Info, State) ->
	{noreply, State}.



%%%===================================================================
%%% Internal functions
%%%===================================================================


next_loop( Pid, Timestamp ) ->
	gen_server:cast( Pid, { loopn, Pid, Timestamp } ).

next_loop( Pid ) ->
	gen_server:cast( Pid, loopn ).




loop( #state{ host = Host, port = Port, topology = Topology, server = Server, conn_to=Timeout, counter=Counter } = State ) ->

	ConnectArgs = [ {host, Host }, {port, Port}, { timeout, Timeout} ],

	try check( ConnectArgs, Server ) of
		Res ->
			gen_server:cast( Topology, Res ),
			next_loop( self(), 10 )
	catch
		_:_ ->
			gen_server:cast( Topology, { server_to_unknown, Server } ),
			next_loop( self(), 1 )
	end,

	State#state{ timer = undefined, counter = Counter + 1 }.




check( ConnectArgs, Server ) ->
	Start = erlang:now(),
	{ok, Conn} = mongo:connect( ConnectArgs ),
	{true, IsMaster} = mongo:command( Conn, { isMaster, 1 } ),
	Finish = erlang:now(),
	mongo:disconnect( Conn ),
	{ monitor_ismaster, Server, IsMaster, timer:now_diff( Finish, Start ) }.


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