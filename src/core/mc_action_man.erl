%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2014, <COMPANY>
%%% @doc  mongo connection action manager.
%%% It processes all write|read|other requests from mongo api
%%% @end
%%% Created : 28. май 2014 18:27
%%%-------------------------------------------------------------------
-module(mc_action_man).
-author("tihon").

-include("mongo_protocol.hrl").

%% API
-export([write/2, read/2, read_one/2, do/5]).

%% @doc Execute mongo action under given write_mode, read_mode, connection, and database.
-spec do(connection(), write_mode(), read_mode(), database(), action(A)) -> A.
do({pool, PoolName}, WriteMode, ReadMode, Database, Action) ->
	Worker = mc_connection_man:request_worker(PoolName),
	try
		do(Worker, WriteMode, ReadMode, Database, Action)
	after mc_connection_man:free_worker(PoolName, Worker)
	end;

do(Connection, WriteMode, ReadMode, Database, Action) ->
	gen_server:call(Connection, #conn_state{database = Database, write_mode = WriteMode, read_mode = ReadMode}),
	Action(Connection).

-spec write(Connection :: pid() | term(), Request :: term()) -> any().
write(Connection, Request) ->
	case Context#context.write_mode of
		unsafe ->
			mc_connection_man:request(Connection, write, Request);
		SafeMode ->
			Params = case SafeMode of safe -> {}; {safe, Param} -> Param end,
			Ack = write(Connection, Request, Params),
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
write(Connection, Request, GetLastErrorParams) ->
	ok = mc_connection_man:request(Connection, Request),
	{0, [Doc | _]} = mc_connection_man:request(Connection, #'query'{
		batchsize = -1,
		collection = '$cmd',
		selector = bson:append({getlasterror, 1}, GetLastErrorParams)
	}),
	Doc.

read(Connection, Request) ->
	#context{database = Database, read_mode = ReadMode} = erlang:get(mongo_action_context), %TODO get rid of process dict
	#'query'{collection = Collection, batchsize = BatchSize} = Request,
	{Cursor, Batch} = mc_connection_man:request(Connection, Request#'query'{slaveok = ReadMode =:= slave_ok}),
	mongo_cursor:create(Connection, Database, Collection, Cursor, BatchSize, Batch).  %TODO when pool Coonection will be {atom, PoolName}!! Resolv me

read_one(Connection, Request) ->
	#context{read_mode = ReadMode} = erlang:get(mongo_action_context), %TODO get rid of process dict
	{0, Docs} = mc_connection_man:request(Connection, Request#'query'{batchsize = -1, slaveok = ReadMode =:= slave_ok}),
	case Docs of
		[] -> {};
		[Doc | _] -> {Doc}
	end.