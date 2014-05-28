%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2014, <COMPANY>
%%% @doc  mongo connection action manager.
%%% it process write|read requests to pool, if pool is used, or to standalone worker, if connection specified.
%%% @end
%%% Created : 28. май 2014 18:27
%%%-------------------------------------------------------------------
-module(mc_action_man).
-author("tihon").

-include("mongo_protocol.hrl").

%% API
-export([write/2, read/2, read_one/2, do/5, request/3, request/2]).

request(Connection, Params) -> % default request
	request(Connection, request, Params).

-spec request(pid() | term(), atom(), term()) -> ok | {non_neg_integer(), [bson:document()]}.
request({pool, PoolName}, Command, Params) ->
	mc_connection_man:request(Command, PoolName, Params);
request(Connection, Command, Params) ->
	mc_connection_man:reply(gen_server:call(Connection, {Command, Params}, infinity)).

%% @doc Execute mongo action under given write_mode, read_mode, connection, and database.
-spec do(connection(), write_mode(), read_mode(), database(), action(A)) -> A.
do({pool, PoolName}, WriteMode, ReadMode, Database, Action) ->
	Worker = mc_connection_man:request_worker(PoolName),
	catch do(Worker, WriteMode, ReadMode, Database, Action),
	mc_connection_man:free_worker(PoolName, Worker);
do(Connection, WriteMode, ReadMode, Database, Action) ->
	gen_server:call(Connection, {update, [{database, Database}, {write_mode, WriteMode}, {read_mode, ReadMode}]}),
	Action(Connection).

-spec write(Connection :: pid() | term(), Request :: term()) -> any().
write(Connection, Request) ->
	case Context#context.write_mode of
		unsafe ->
			write(Context#context.connection, Context#context.database, Request); %TODO move me to mc_worker
		SafeMode ->
			Params = case SafeMode of safe -> {}; {safe, Param} -> Param end,
			Ack = write(Connection, Context#context.database, Request, Params),
			case bson:lookup(err, Ack, undefined) of
				undefined -> ok;
				String ->
					case bson:at(code, Ack) of
						10058 -> erlang:exit(not_master);
						Code -> erlang:exit({write_failure, Code, String})
					end
			end
	end.

%% @private
write({pool, PoolName}, Database, Request) ->
	mc_connection_man:request(PoolName, Database, Request);
write(Connection, Database, Request) ->
	mc_worker:request(Connection, Database, Request).

%% @private
write({pool, PoolName}, Database, Request, GetLastErrorParams) -> %TODO remove code duplicates
	ok = mc_connection_man:request(PoolName, Database, Request),
	{0, [Doc | _]} = mc_connection_man:request(PoolName, Database, #'query'{
		batchsize = -1,
		collection = '$cmd',
		selector = bson:append({getlasterror, 1}, GetLastErrorParams)
	}),
	Doc;
write(Connection, Database, Request, GetLastErrorParams) ->
	ok = mc_worker:request(Connection, Database, Request),
	{0, [Doc | _]} = mc_worker:request(Connection, Database, #'query'{
		batchsize = -1,
		collection = '$cmd',
		selector = bson:append({getlasterror, 1}, GetLastErrorParams)
	}),
	Doc.

read({pool, PoolName}, Request) ->
	#context{database = Database, read_mode = ReadMode} = erlang:get(mongo_action_context), %TODO get rid of process dict
	#'query'{collection = Collection, batchsize = BatchSize} = Request,
	{Cursor, Batch} = mc_connection_man:request(PoolName, Database, Request#'query'{slaveok = ReadMode =:= slave_ok}),
	mongo_cursor:create(ok, Database, Collection, Cursor, BatchSize, Batch);  %TODO resolve me (Pid instead ok).
read(Connection, Request) ->
	#context{database = Database, read_mode = ReadMode} = erlang:get(mongo_action_context), %TODO get rid of process dict
	#'query'{collection = Collection, batchsize = BatchSize} = Request,
	{Cursor, Batch} = mc_worker:request(Connection, Database, Request#'query'{slaveok = ReadMode =:= slave_ok}),
	mongo_cursor:create(Connection, Database, Collection, Cursor, BatchSize, Batch).

read_one({pool, PoolName}, Request) ->
	#context{database = Database, read_mode = ReadMode} = erlang:get(mongo_action_context), %TODO get rid of process dict
	{0, Docs} = mc_connection_man:request(PoolName, Database, Request#'query'{batchsize = -1, slaveok = ReadMode =:= slave_ok}),
	case Docs of
		[] -> {};
		[Doc | _] -> {Doc}
	end;
read_one(Connection, Request) ->
	#context{database = Database, read_mode = ReadMode} = erlang:get(mongo_action_context), %TODO get rid of process dict
	{0, Docs} = mc_worker:request(Connection, Database, Request#'query'{batchsize = -1, slaveok = ReadMode =:= slave_ok}),
	case Docs of
		[] -> {};
		[Doc | _] -> {Doc}
	end.