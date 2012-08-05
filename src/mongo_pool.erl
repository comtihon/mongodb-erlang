-module(mongo_pool).
-export([
	start/3,
	start_link/3,
	get/1
]).

-behaviour(gen_server).
-export([
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3
]).

-record(state, {
	supervisor  :: pid(),
	connections :: array(),
	monitors    :: orddict:orddict()
}).


-spec start(atom(), pos_integer(), pid()) -> {ok, pid()}.
start(Name, Size, Sup) ->
	gen_server:start({local, Name}, ?MODULE, [Size, Sup], []).

-spec start_link(atom(), pos_integer(), pid()) -> {ok, pid()}.
start_link(Name, Size, Sup) ->
	gen_server:start_link({local, Name}, ?MODULE, [Size, Sup], []).

-spec get(atom() | pid()) -> mongo_connection:connection().
get(Pool) ->
	case gen_server:call(Pool, get, infinity) of
		{ok, Connection} -> Connection;
		{error, Reason} -> erlang:exit(Reason)
	end.

%% @hidden
init([Size, Sup]) ->
	random:seed(erlang:now()),
	{ok, #state{
		supervisor = Sup,
		connections = array:new(Size, [{fixed, false}, {default, undefined}]),
		monitors = orddict:new()
	}}.

%% @hidden
handle_call(get, _From, #state{connections = Connections} = State) ->
	Index = random:uniform(array:size(Connections)) - 1,
	case array:get(Index, Connections) of
		undefined ->
			try supervisor:start_child(State#state.supervisor, []) of
				{ok, Connection} ->
					Monitor = erlang:monitor(process, Connection),
					{reply, {ok, Connection}, State#state{
						connections = array:set(Index, Connection, Connections),
						monitors = orddict:store(Monitor, Index, State#state.monitors)
					}}
			catch
				_:Error -> {reply, {error, Error}, State}
			end;
		Connection ->
			{reply, {ok, Connection}, State}
	end.

%% @hidden
handle_cast(_, State) ->
	{noreply, State}.

%% @hidden
handle_info({'DOWN', Monitor, process, _Pid, _}, #state{monitors = Monitors} = State) ->
	{ok, Index} = orddict:find(Monitor, Monitors),
	{noreply, State#state{
		connections = array:set(Index, undefined, State#state.connections),
		monitors = orddict:erase(Index, Monitors)
	}};

handle_info(_, State) ->
	{noreply, State}.

%% @hidden
terminate(_, _State) ->
	ok.

%% @hidden
code_change(_Old, State, _Extra) ->
	{ok, State}.
