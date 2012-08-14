-module(mongo_cursor).
-export([
	create/6,
	next/1,
	rest/1,
	take/2,
	close/1
]).

-export([
	start_link/1
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
	connection  :: mongo_connection:connection(),
	database    :: atom(),
	collection  :: atom(),
	cursor      :: integer(),
	batchsize   :: integer(),
	batch       :: [bson:document()],
	monitor     :: reference
}).

-include ("mongo_protocol.hrl").


-spec create(mongo_connection:connection(), atom(), atom(), integer(), integer(), [bson:document()]) -> pid().
create(Connection, Database, Collection, Cursor, BatchSize, Batch) ->
	{ok, Pid} = mongo_sup:start_cursor([self(), Connection, Database, Collection, Cursor, BatchSize, Batch]),
	Pid.

-spec next(pid()) -> {} | {bson:document()}.
next(Cursor) ->
	try gen_server:call(Cursor, next, infinity) of
		Result -> Result
	catch
		exit:{noproc, _} -> {}
	end.

-spec rest(pid()) -> [bson:document()].
rest(Cursor) ->
	try gen_server:call(Cursor, rest, infinity) of
		Result -> Result
	catch
		exit:{noproc, _} -> []
	end.

-spec take(pid(), non_neg_integer()) -> [bson:document()].
take(Cursor, Limit) ->
	try gen_server:call(Cursor, {rest, Limit}, infinity) of
		Result -> Result
	catch
		exit:{noproc, _} -> []
	end.

-spec close(pid()) -> ok.
close(Cursor) ->
	catch gen_server:call(Cursor, stop),
	ok.

start_link(Args) ->
	gen_server:start_link(?MODULE, Args, []).


%% @hidden
init([Owner, Connection, Database, Collection, Cursor, BatchSize, Batch]) ->
	{ok, #state{
		connection = Connection,
		database = Database,
		collection = Collection,
		cursor = Cursor,
		batchsize = BatchSize,
		batch = Batch,
		monitor = erlang:monitor(process, Owner)
	}}.

%% @hidden
handle_call(next, _From, State) ->
	case next_i(State) of
		{Reply, #state{cursor = 0, batch = []} = UpdatedState} ->
			{stop, normal, Reply, UpdatedState};
		{Reply, UpdatedState} ->
			{reply, Reply, UpdatedState}
	end;

handle_call(rest, _From, State) ->
	{Reply, UpdatedState} = rest_i(State, infinity),
	{stop, normal, Reply, UpdatedState};

handle_call({rest, Limit}, _From, State) ->
	case rest_i(State, Limit) of
		{Reply, #state{cursor = 0} = UpdatedState} ->
			{stop, normal, Reply, UpdatedState};
		{Reply, UpdatedState} ->
			{reply, Reply, UpdatedState}
	end;

handle_call(stop, _From, State) ->
	{stop, normal, ok, State}.

%% @hidden
handle_cast(_, State) ->
	{noreply, State}.

%% @hidden
handle_info({'DOWN', Monitor, process, _, _}, #state{monitor = Monitor} = State) ->
	{stop, normal, State};

handle_info(_, State) ->
	{noreply, State}.

%% @hidden
terminate(_, #state{cursor = 0}) ->
	ok;
terminate(_, State) ->
	mongo_connection:request(State#state.connection, State#state.database, #killcursor{
		cursorids = [State#state.cursor]
	}).

%% @hidden
code_change(_Old, State, _Extra) ->
	{ok, State}.

%% @private
next_i(#state{batch = [Doc | Rest]} = State) ->
	{{Doc}, State#state{batch = Rest}};
next_i(#state{batch = [], cursor = 0} = State) ->
	{{}, State};
next_i(#state{batch = []} = State) ->
	{Cursor, Batch} = mongo_connection:request(State#state.connection, State#state.database, #getmore{
		collection = State#state.collection,
		batchsize = State#state.batchsize,
		cursorid = State#state.cursor
	}),
	next_i(State#state{cursor = Cursor, batch = Batch}).

%% @private
rest_i(State, infinity) ->
	rest_i(State, -1);
rest_i(State, Limit) ->
	{Docs, UpdatedState} = rest_i(State, [], Limit),
	{lists:reverse(Docs), UpdatedState}.

%% @private
rest_i(State, Acc, 0) ->
	{Acc, State};
rest_i(State, Acc, Limit) ->
	case next_i(State) of
		{{}, UpdatedState} -> {Acc, UpdatedState};
		{{Doc}, UpdatedState} -> rest_i(UpdatedState, [Doc | Acc], Limit - 1)
	end.
