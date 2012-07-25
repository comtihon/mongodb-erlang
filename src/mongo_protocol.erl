-module(mongo_protocol).
-export([
	dbcoll/2,
	put_message/3,
	get_reply/1
]).
-export_type([db/0]).
-export_type([notice/0, request/0, reply/0]).
-export_type([message/0]).
-export_type([requestid/0]).

-include("mongo_protocol.hrl").
-include_lib("bson/include/bson_binary.hrl").

% A notice is an asynchronous message sent to the server (no reply expected)
-type notice() :: #insert{} | #update{} | #delete{} | #killcursor{}.
% A request is a syncronous message sent to the server (reply expected)
-type request() :: #'query'{} | #getmore{}.
% A reply to a request
-type reply() :: #reply{}.
 % message id
-type requestid() :: integer().
-type message() :: notice() | request().


% RequestId expected to be in scope at call site
-define(put_header(Opcode), ?put_int32(RequestId), ?put_int32(0), ?put_int32(Opcode)).
-define(get_header(Opcode, ResponseTo), ?get_int32(_RequestId), ?get_int32(ResponseTo), ?get_int32(Opcode)).

-define(ReplyOpcode, 1).
-define(UpdateOpcode, 2001).
-define(InsertOpcode, 2002).
-define(QueryOpcode, 2004).
-define(GetmoreOpcode, 2005).
-define(DeleteOpcode, 2006).
-define(KillcursorOpcode, 2007).


-spec dbcoll (db(), collection()) -> bson:utf8().
%@doc Concat db and collection name with period (.) in between
dbcoll (Db, Coll) -> <<(atom_to_binary (Db, utf8)) /binary, $., (atom_to_binary (Coll, utf8)) /binary>>.

-spec put_message(db(), message(), requestid()) -> binary().
put_message(Db, #insert{collection = Coll, documents = Docs}, RequestId) ->
	<<?put_header(?InsertOpcode),
		?put_int32(0),
		(bson_binary:put_cstring(dbcoll(Db, Coll))) /binary,
		<< <<(bson_binary:put_document(Doc))/binary>> || Doc <- Docs>>/binary >>;
put_message(Db, #update{collection = Coll, upsert = U, multiupdate = M, selector = Sel, updater = Up}, RequestId) ->
	<<?put_header(?UpdateOpcode),
		?put_int32(0),
		(bson_binary:put_cstring(dbcoll(Db, Coll)))/binary,
		?put_bits32(0,0,0,0,0,0, bit(M), bit(U)),
		(bson_binary:put_document(Sel))/binary,
		(bson_binary:put_document (Up))/binary>>;
put_message(Db, #delete{collection = Coll, singleremove = R, selector = Sel}, RequestId) ->
	<<?put_header(?DeleteOpcode),
		?put_int32(0),
		(bson_binary:put_cstring(dbcoll(Db, Coll)))/binary,
		?put_bits32(0,0,0,0,0,0,0, bit(R)),
		(bson_binary:put_document(Sel))/binary>>;
put_message(_Db, #killcursor{cursorids = Cids}, RequestId) ->
	<<?put_header(?KillcursorOpcode),
		?put_int32(0),
		?put_int32(length(Cids)),
		<< <<?put_int64(Cid)>> || Cid <- Cids>>/binary >>;
put_message(Db, #'query'{tailablecursor = TC, slaveok = SOK, nocursortimeout = NCT, awaitdata = AD,
		collection = Coll, skip = Skip, batchsize = Batch, selector = Sel, projector = Proj}, RequestId) ->
	<<?put_header(?QueryOpcode),
		?put_bits32(0, 0, bit(AD), bit(NCT), 0, bit(SOK), bit(TC), 0),
		(bson_binary:put_cstring (dbcoll (Db, Coll)))/binary,
		?put_int32(Skip),
		?put_int32(Batch),
		(bson_binary:put_document(Sel))/binary,
		(case Proj of [] -> <<>>; _ -> bson_binary:put_document(Proj) end)/binary >>;
put_message(Db, #getmore{collection = Coll, batchsize = Batch, cursorid = Cid}, RequestId) ->
	<<?put_header(?GetmoreOpcode),
		?put_int32 (0),
		(bson_binary:put_cstring(dbcoll (Db, Coll))) /binary,
		?put_int32(Batch),
		?put_int64(Cid)>>.


-spec get_reply(binary()) -> {requestid(), reply(), binary()}.
get_reply(Message) ->
	<<?get_header(?ReplyOpcode, ResponseTo),
		?get_bits32 (_,_,_,_, AwaitCapable, _, QueryError, CursorNotFound),
		?get_int64 (CursorId),
		?get_int32 (StartingFrom),
		?get_int32 (NumDocs),
		Bin /binary >> = Message,
	{Docs, BinRest} = get_docs (NumDocs, Bin),
	Reply = #reply {
		cursornotfound = bool(CursorNotFound),
		queryerror = bool(QueryError),
		awaitcapable = bool(AwaitCapable),
		cursorid = CursorId,
		startingfrom = StartingFrom,
		documents = Docs
	},
	{ResponseTo, Reply, BinRest}.


%% @private
get_docs(0, Bin) -> {[], Bin};
get_docs(NumDocs, Bin) when NumDocs > 0 ->
	{Doc, Bin1} = bson_binary:get_document (Bin),
	{Docs, Bin2} = get_docs (NumDocs - 1, Bin1),
	{[Doc | Docs], Bin2}.

%% @private
bit(false) -> 0;
bit(true) -> 1.

%% @private
bool(0) -> false;
bool(1) -> true.
