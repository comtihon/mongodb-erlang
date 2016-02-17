%%%-------------------------------------------------------------------
%%% @author Alexander Hudich (alttagil@gmail.com)
%%% @doc
%%% Client for a MongoDB instance, a replica set, or a set of mongoses.
%%% @end
%%%-------------------------------------------------------------------
-module(mongoc).
-author("alttagil@gmail.com").

-include("mongo_protocol.hrl").

-export([connect/3, disconnect/1, command/2, command/3, insert/3, update/4, delete/3, delete_one/3, find_one/3, find_one/4, find/4, find/3, count/3, count/4, count/5, ensure_index/3, update/5]).


-type selector() :: bson:document().
-type cursor() :: pid().
-type write_mode() :: unsafe | safe | {safe, bson:document()}.

-type readmode() :: primary | secondary | primaryPreferred | secondaryPreferred | nearest.
-type host() :: list().
-type seed() :: host()
| { rs, binary(), [host()] }
| { single, host() }
| { unknown, [host()] }
| { sharded, [host()] }.
-type connectoptions() :: [coption()].
-type coption() :: {name, atom()}
|{minPoolSize, integer()}
|{maxPoolSize, integer()}
|{localThresholdMS,integer()}
|{connectTimeoutMS,integer()}
|{socketTimeoutMS,integer()}
|{serverSelectionTimeoutMS,integer()}
|{waitQueueTimeoutMS,integer()}
|{heartbeatFrequencyMS, integer()}
|{minHeartbeatFrequencyMS, integer()}
|{rp_mode,readmode()}
|{rp_tags,list() }.
-type workeroptions() :: [woption()].
-type woption() :: {database, database()}
| {login, binary()}
| {password, binary()}
| {w_mode, write_mode()}.
-type readprefs() :: [readpref()].
-type readpref() :: {rp_mode, readmode()}
|{rp_tags, [tuple()]}.
-type reason() :: atom().


-spec connect(seed(), connectoptions(), workeroptions()) -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
-spec disconnect(pid()) -> ok.
-spec insert(pid(), colldb(), A) -> A | { error, reason() }.
-spec update(pid(), colldb(), selector(), bson:document()) -> ok | { error, reason() }.
-spec update(pid(), colldb(), selector(), bson:document(), proplists:proplist()) -> ok | { error, reason() }.
-spec delete(pid(), colldb(), selector()) -> ok | { error, reason() }.
-spec delete_one(pid(), colldb(), selector()) -> ok | { error, reason() }.
-spec find_one(pid(), colldb(), selector()) -> {} | {bson:document()}.
-spec find_one(pid(), colldb(), selector(), readprefs()) -> {} | {bson:document()}.
-spec find(pid(), colldb(), selector()) -> cursor().
-spec find(pid(), colldb(), selector(), readprefs()) -> cursor().
-spec count(pid(), colldb(), selector()) -> integer().
-spec count(pid(), colldb(), selector(), readprefs()) -> integer().
-spec count(pid(), colldb(), selector(), readprefs(), integer()) -> integer().
-spec ensure_index(pid(), colldb(), bson:document()) -> ok | { error, reason() }.
-spec command(pid(), bson:document()) -> {boolean(), bson:document()} | { error, reason() }. % Action
-spec command(pid(), bson:document(), readprefs()) -> {boolean(), bson:document()} | { error, reason() }. % Action


% mongoc:connect( stat,

%					"localhost:27017",
%					{ single, "localhost:27017" },
%					{ rs, <<"rs0">>, [ "localhost:27017", "localhost:27018"] }
%					{ unknown, [ "localhost:27017", "localhost:27018"] }
%					{ sharded, [ "localhost:27017", "localhost:27018"] }

%						[
%							{ name,  mongo1 },

%							{ minPoolSize, 5 },
%							{ maxPoolSize, 10 },

%							{ localThresholdMS, 1000 },

%							{ connectTimeoutMS, 20000 },
%							{ socketTimeoutMS, 100 },
%							{ serverSelectionTimeoutMS, 30000 },
%							{ waitQueueTimeoutMS, 1000 },
%							{ heartbeatFrequencyMS, 10000 },
%							{ minHeartbeatFrequencyMS, 1000 },

%							{ rp_mode, primary },
%							{ rp_tags, [{tag1,1},{tag2,2}] },

%						]

%						[
%							{login, binary()}
%							{password, binary()}
%							{database, binary()}
%							{w_mode, write_mode()}
%						],

%			).

%% @doc Creates new topology discoverer, return its pid

connect( Seeds, Options, WorkerOptions ) ->
	mc_topology:start_link( Seeds, Options, WorkerOptions ).


disconnect( Topology ) ->
	mc_topology:disconnect( Topology ).



%% @doc Inserts a document or multiple documents into a collection.
%%      Returns the document or documents with an auto-generated _id if missing.

insert( Topology, Coll, Doc ) ->
	case mc_topology:get_pool( Topology, [{rp_mode, primary}] ) of
		{ ok, #{ pool := C } } ->
			try	mongo:insert( C, Coll, Doc ) of
				R -> R
			catch
				error:not_master ->
					mc_topology:update_topology( Topology ),
					{ error, not_master };
				error:{bad_query,{not_master,_}}  ->
					mc_topology:update_topology( Topology ),
					{ error, not_master };
				_:R ->
					mc_topology:update_topology( Topology ),
					{error, R}
			end;
		Error ->
			Error
	end.


%% @doc Replaces the document matching criteria entirely with the new Document.

update( Topology, Coll, Selector, Doc ) ->
	update( Topology, Coll, Selector, Doc, [] ).

update( Topology, Coll, Selector, Doc, Options ) ->
	case mc_topology:get_pool( Topology, [{rp_mode, primary}] ) of
		{ ok, #{ pool := C } } ->
			try	mongo:update( C, Coll, Selector, Doc, Options) of
				R -> R
			catch
				error:not_master ->
					mc_topology:update_topology( Topology ),
					{ error, not_master };
				error:{bad_query,{not_master,_}}  ->
					mc_topology:update_topology( Topology ),
					{ error, not_master }
			end;
		Error ->
			Error
	end.


%% @doc Deletes selected documents

delete(Topology, Coll, Selector) ->
	case mc_topology:get_pool( Topology, [{rp_mode, primary}] ) of
		{ ok, #{ pool := C } } ->
			try	mongo:delete( C, Coll, Selector ) of
				R -> R
			catch
				error:not_master ->
					mc_topology:update_topology( Topology ),
					{ error, not_master };
				error:{bad_query,{not_master,_}}  ->
					mc_topology:update_topology( Topology ),
					{ error, not_master }
			end;
		Error ->
			Error
	end.


%% @doc Deletes first selected document.

delete_one(Topology, Coll, Selector) ->
	case mc_topology:get_pool( Topology, [{rp_mode, primary}] ) of
		{ ok, #{ pool := C } } ->
			try	mongo:delete_one( C, Coll, Selector ) of
				R -> R
			catch
				error:not_master ->
					mc_topology:update_topology( Topology ),
					{ error, not_master };
				error:{bad_query,{not_master,_}}  ->
					mc_topology:update_topology( Topology ),
					{ error, not_master }
			end;
		Error ->
			Error
	end.


%% @doc Returns first selected document, if any

find_one(Topology, Coll, Selector) ->
	find_one(Topology, Coll, Selector, []).

find_one(Topology, Coll, Selector, Options) ->
	case mc_topology:get_pool( Topology, Options ) of
		{ ok, #{ pool := C, server_type := Type, readPreference := RPrefs } } ->
			Projector = mc_utils:get_value(projector, Options, []),
			Skip = mc_utils:get_value(skip, Options, 0),
			Q = #'query'{
				collection = Coll,
				selector = Selector,
				projector = Projector,
				skip = Skip
			},
			mc_action_man:read_one(C, mongos_query_transform( Type, Q, RPrefs ) );
		Error ->
			Error
	end.


%% @doc Returns projection of selected documents.
%%      Empty projection [] means full projection.

find(Topology, Coll, Selector) ->
	find(Topology, Coll, Selector, []).

find(Topology, Coll, Selector, Options) ->
	case mc_topology:get_pool( Topology, Options ) of
		{ ok, #{ pool := C, server_type := Type, readPreference := RPrefs } } ->
			Projector = mc_utils:get_value(projector, Options, []),
			Skip = mc_utils:get_value(skip, Options, 0),
			BatchSize = mc_utils:get_value(batchsize, Options, 0),
			Q = #'query'{
				collection = Coll,
				selector = Selector,
				projector = Projector,
				skip = Skip,
				batchsize = BatchSize
			},
			mc_action_man:read(C, mongos_query_transform( Type, Q, RPrefs ) );
		Error ->
			Error
	end.



%% @doc Creates index on collection according to given spec.
%%      The key specification is a bson documents with the following fields:
%%      key      :: bson document, for e.g. {field, 1, other, -1, location, 2d}, <strong>required</strong>
%%      name     :: bson:utf8()
%%      unique   :: boolean()
%%      dropDups :: boolean()

ensure_index( Topology, Coll, IndexSpec ) ->
	case mc_topology:get_pool( Topology, [{rp_mode, primary}] ) of
		{ ok, #{ pool := C } } ->
			try	mc_connection_man:request_async(C, #ensure_index{collection = Coll, index_spec = IndexSpec}) of
				R -> R
			catch
				error:not_master ->
					mc_topology:update_topology( Topology ),
					{ error, not_master };
				error:{bad_query,{not_master,_}}  ->
					mc_topology:update_topology( Topology ),
					{ error, not_master }
			end;
		Error ->
			Error
	end.



%% @doc Counts selected documents

count( Topology, Coll, Selector ) ->
	count(Topology, Coll, Selector, [], 0).

count( Topology, Coll, Selector, Options ) ->
	count(Topology, Coll, Selector, Options, 0).


%% @doc Count selected documents up to given max number; 0 means no max.
%%     Ie. stops counting when max is reached to save processing time.

count( Topology, {Db, Coll}, Selector, Options, Limit ) when Limit =< 0 ->
	{true, #{<<"n">> := N}} = command(Topology, {<<"count">>, mc_utils:value_to_binary(Coll), <<"query">>, Selector}, Options, Db),
	trunc(N);
count( Topology, {Db, Coll}, Selector, Options, Limit ) ->
	{true, #{<<"n">> := N}} = command(Topology, {<<"count">>, mc_utils:value_to_binary(Coll), <<"query">>, Selector, <<"limit">>, Limit}, Options, Db ),
	trunc(N); % Server returns count as float

count( Topology, Coll, Selector, Options, Limit ) when Limit =< 0 ->
	{true, #{<<"n">> := N}} = command(Topology, {<<"count">>, mc_utils:value_to_binary(Coll), <<"query">>, Selector}, Options),
	trunc(N);
count( Topology, Coll, Selector, Options, Limit ) ->
	{true, #{<<"n">> := N}} = command(Topology, {<<"count">>, mc_utils:value_to_binary(Coll), <<"query">>, Selector, <<"limit">>, Limit}, Options ),
	trunc(N). % Server returns count as float








command( Topology, Command ) ->
	command( Topology, Command, [] ).

command( Topology, Command, Options ) ->
	command( Topology, Command, Options, undefined ).

command( Topology, Command, Options, Db ) ->
	case mc_topology:get_pool( Topology, Options ) of
		{ ok, #{ pool := C, server_type := Type, readPreference := RPrefs } } ->
			Q = #'query'{
				collection = { Db, <<"$cmd">> },
				selector = Command
			},

			try	exec_command( C, mongos_query_transform( Type, Q, RPrefs ) ) of
				R -> R
			catch
				error:not_master ->
					mc_topology:update_topology( Topology ),
					{ error, not_master };
				error:{bad_query,{not_master,_}}  ->
					mc_topology:update_topology( Topology ),
					{ error, not_master }
			end;
		Error ->
			Error
	end.


% @private
exec_command( C, Command ) ->
	Doc = mc_action_man:read_one( C, Command ),
	mc_connection_man:process_reply( Doc, Command ).




%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private

mongos_query_transform( mongos, #'query'{ selector = S } = Q, #{ mode := primary } ) ->
	Q#'query'{ selector = S, slaveok = false, sok_overriden = true };

mongos_query_transform( mongos, #'query'{ selector = S } = Q, #{ mode := primaryPreferred, tags := [] } ) ->
	Q#'query'{ selector = S, slaveok = true, sok_overriden = true };

mongos_query_transform( mongos, #'query'{ selector = S } = Q, #{ mode := primaryPreferred, tags := Tags } ) ->
	Q#'query'{ selector = add_rp( S, { '$readPreference', { mode, <<"primaryPreferred">>, tags, bson:document( Tags ) } } ), slaveok = true, sok_overriden = true };

mongos_query_transform( mongos, #'query'{ selector = S } = Q, #{ mode := secondary, tags := [] } ) ->
	Q#'query'{ selector = add_rp( S, { '$readPreference', { mode, <<"secondary">> } } ), slaveok = true, sok_overriden = true };

mongos_query_transform( mongos, #'query'{ selector = S } = Q, #{ mode := secondary, tags := Tags } ) ->
	Q#'query'{ selector = add_rp( S, { '$readPreference', { mode, <<"secondary">>, tags, bson:document( Tags ) } } ), slaveok = true, sok_overriden = true };

mongos_query_transform( mongos, #'query'{ selector = S } = Q, #{ mode := secondaryPreferred, tags := [] } ) ->
	Q#'query'{ selector = add_rp( S, { '$readPreference', { mode, <<"secondaryPreferred">> } } ), slaveok = true, sok_overriden = true };

mongos_query_transform( mongos, #'query'{ selector = S } = Q, #{ mode := secondaryPreferred, tags := Tags } ) ->
	Q#'query'{ selector = add_rp( S, { '$readPreference', { mode, <<"secondaryPreferred">>, tags, bson:document( Tags ) } } ), slaveok = true, sok_overriden = true };

mongos_query_transform( mongos, #'query'{ selector = S } = Q, #{ mode := nearest, tags := [] } ) ->
	Q#'query'{ selector = add_rp( S, { '$readPreference', { mode, <<"nearest">> } } ), slaveok = true, sok_overriden = true };

mongos_query_transform( mongos, #'query'{ selector = S } = Q, #{ mode := nearest, tags := Tags } ) ->
	Q#'query'{ selector = add_rp( S, { '$readPreference', { mode, <<"nearest">>, tags, bson:document( Tags ) } } ), slaveok = true, sok_overriden = true };

mongos_query_transform( _, Q, #{ mode := primary } ) ->
	Q#'query'{ slaveok = false, sok_overriden = true };

mongos_query_transform( _, Q, _ ) ->
	Q#'query'{ slaveok = true, sok_overriden = true }.


add_rp( Selector, RP )->
	add_rp( is_query( Selector ), Selector, RP ).

add_rp( false, Selector, RP ) ->
	list_to_tuple( [ '$query', Selector ] ++ tuple_to_list( RP ) );
add_rp( true, Selector, RP ) ->
	list_to_tuple( tuple_to_list( Selector ) ++ tuple_to_list( RP ) ).


is_query( {} ) ->
	false;
is_query( Selector ) when is_tuple( Selector ) ->
	L = tuple_to_list( Selector ),
	( lists:member( '$query', L ) or lists:member( <<"$query">>, L ) ).