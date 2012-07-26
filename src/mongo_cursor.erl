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
	batch       :: [bson:document()]
}).

-include ("mongo_protocol.hrl").


-spec create(mongo_connection:connection(), atom(), atom(), integer(), integer(), [bson:document()]) -> pid().
create(Connection, Database, Collection, Cursor, BatchSize, Batch) ->
	mongo_sup:start_child(cursor, [Connection, Database, Collection, Cursor, BatchSize, Batch]).

-spec next(pid()) -> {} | {bson:document()}.
next(Cursor) ->
	gen_server:call(Cursor, next).

-spec rest(pid()) -> [bson:document()].
rest(Cursor) ->
	gen_server:call(Cursor, rest).

-spec take(pid(), non_neg_integer()) -> [bson:document()].
take(Cursor, Limit) ->
	gen_server:call(Cursor, {rest, Limit}).

-spec close(pid()) -> ok.
close(Cursor) ->
	catch gen_server:call(Cursor, close),
	ok.

start_link(Args) ->
	gen_server:start_link(?MODULE, Args, []).


%% @hidden
init([Connection, Database, Collection, Cursor, BatchSize, Batch]) ->
	{ok, #state{
		connection = Connection,
		database = Database,
		collection = Collection,
		cursor = Cursor,
		batchsize = BatchSize,
		batch = Batch
	}}.

%% @hidden
handle_call(next, _From, State) ->
	{Reply, UpdatedState} = next_i(State),
	{reply, Reply, UpdatedState};

handle_call(rest, _From, State) ->
	{Reply, UpdatedState} = rest_i(State, infinity),
	{reply, Reply, UpdatedState};

handle_call({rest, Limit}, _From, State) ->
	{Reply, UpdatedState} = rest_i(State, Limit),
	{reply, Reply, UpdatedState};

handle_call(stop, _From, State) ->
	{stop, normal, State}.

%% @hidden
handle_cast(_, State) ->
	{noreply, State}.

%% @hidden
handle_info(_, State) ->
	{noreply, State}.

%% @hidden
terminate(_, State) ->
	ok = mongo_connection:request(State#state.connection, State#state.database, #killcursor{
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
