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
-export([write/2, read/2, read_one/2]).

-spec write(Connection :: pid() | term(), Request) -> any().
write(Connection, Request) ->
	Context = erlang:get(mongo_action_context), %TODO remove process dictionary usage
	case Context#context.write_mode of
		unsafe ->
			write(Context#context.connection, Context#context.database, Request);
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
	mongo_connection_worker:request(Connection, Database, Request).

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
	ok = mongo_connection_worker:request(Connection, Database, Request),
	{0, [Doc | _]} = mongo_connection_worker:request(Connection, Database, #'query'{
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
	{Cursor, Batch} = mongo_connection_worker:request(Connection, Database, Request#'query'{slaveok = ReadMode =:= slave_ok}),
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
	{0, Docs} = mongo_connection_worker:request(Connection, Database, Request#'query'{batchsize = -1, slaveok = ReadMode =:= slave_ok}),
	case Docs of
		[] -> {};
		[Doc | _] -> {Doc}
	end.