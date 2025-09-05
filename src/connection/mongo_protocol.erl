-module(mongo_protocol).
-export([
  dbcoll/2,
  put_message/3,
  get_reply/1
]).
-export_type([notice/0, request/0, reply/0]).
-export_type([message/0]).
-export_type([requestid/0]).

-include("mongo_protocol.hrl").
-include_lib("bson/include/bson_binary.hrl").

% A notice is an asynchronous message sent to the server (no reply expected)
-type notice() :: #insert{} | #update{} | #delete{} | #killcursor{} | #ensure_index{}.
% A request is a synchronous message sent to the server (reply expected)
-type request() :: #'query'{} | #getmore{}.
% A reply to a request
-type reply() :: #reply{}.
% message id
-type requestid() :: integer().
-type message() :: notice() | request() | #op_msg_command{} | #op_msg_write_op{}.

% RequestId expected to be in scope at call site
-define(put_header(Opcode), ?put_int32(_RequestId), ?put_int32(0), ?put_int32(Opcode)).
-define(get_header(Opcode, ResponseTo), ?get_int32(_RequestId), ?get_int32(ResponseTo), ?get_int32(Opcode)).
-define(get_header_ignore_req_id(Opcode, ResponseTo), ?get_int32(_), ?get_int32(ResponseTo), ?get_int32(Opcode)).

-define(ReplyOpcode, 1).
-define(UpdateOpcode, 2001).
-define(InsertOpcode, 2002).
-define(QueryOpcode, 2004).
-define(GetmoreOpcode, 2005).
-define(DeleteOpcode, 2006).
-define(KillcursorOpcode, 2007).
-define(OpMsgOpcode, 2013).

-define(OpMsgDbFieldIndex, 4).

-define (put_uint32 (N), (N):32/unsigned-little).
-define (put_uint8 (N), (N):8/unsigned-little).
-define (get_uint32 (N), N:32/unsigned-little).
-define (get_uint8 (N), N:8/unsigned-little).

-spec dbcoll(database(), colldb()) -> bson:utf8().

%@doc Concat db and collection name with period (.) in between
dbcoll(Db, {undefined, Coll}) ->
  dbcoll(Db, Coll);
dbcoll(_, {Db, Coll}) ->
  dbcoll(Db, Coll);
dbcoll(Db, Coll) ->
  <<(binarize(Db))/binary, $., (binarize(Coll))/binary>>.

-spec put_message(mc_worker_api:database(), message(), requestid()) -> binary().
put_message(Db, #insert{collection = Coll, documents = Docs}, _RequestId) ->
  <<?put_header(?InsertOpcode),
  ?put_int32(0),
  (bson_binary:put_cstring(dbcoll(Db, Coll)))/binary,
  <<<<(bson_binary:put_document(Doc))/binary>> || Doc <- Docs>>/binary>>;
put_message(Db, #update{collection = Coll, upsert = U, multiupdate = M, selector = Sel, updater = Up}, _RequestId) ->
  <<?put_header(?UpdateOpcode),
  ?put_int32(0),
  (bson_binary:put_cstring(dbcoll(Db, Coll)))/binary,
  ?put_bits32(0, 0, 0, 0, 0, 0, bit(M), bit(U)),
  (bson_binary:put_document(Sel))/binary,
  (bson_binary:put_document(Up))/binary>>;
put_message(Db, #delete{collection = Coll, singleremove = R, selector = Sel}, _RequestId) ->
  <<?put_header(?DeleteOpcode),
  ?put_int32(0),
  (bson_binary:put_cstring(dbcoll(Db, Coll)))/binary,
  ?put_bits32(0, 0, 0, 0, 0, 0, 0, bit(R)),
  (bson_binary:put_document(Sel))/binary>>;
put_message(_Db, #killcursor{cursorids = Cids}, _RequestId) ->
  <<?put_header(?KillcursorOpcode),
  ?put_int32(0),
  ?put_int32(length(Cids)),
  <<<<?put_int64(Cid)>> || Cid <- Cids>>/binary>>;
put_message(Db, #'query'{tailablecursor = TC, slaveok = SOK, nocursortimeout = NCT, awaitdata = AD,
  collection = Coll, skip = Skip, batchsize = Batch, selector = Sel, projector = Proj}, _RequestId) ->
  <<?put_header(?QueryOpcode),
  ?put_bits32(0, 0, bit(AD), bit(NCT), 0, bit(SOK), bit(TC), 0),
  (bson_binary:put_cstring(dbcoll(Db, Coll)))/binary,
  ?put_int32(Skip),
  ?put_int32(Batch),
  (bson_binary:put_document(Sel))/binary,
  (add_proj(Proj))/binary>>;
put_message(Db, #getmore{collection = Coll, batchsize = Batch, cursorid = Cid}, _RequestId) ->
  <<?put_header(?GetmoreOpcode),
  ?put_int32(0),
  (bson_binary:put_cstring(dbcoll(Db, Coll)))/binary,
  ?put_int32(Batch),
  ?put_int64(Cid)>>;
put_message(Db, #op_msg_write_op{} = OpMsg, _RequestId) ->
  <<
    ?put_header(?OpMsgOpcode),
    ?put_uint32(0), % Flags
    (put_section_type_zero(OpMsg#op_msg_write_op{database = make_bin(Db)}))/binary
  >>;
put_message(Db, #op_msg_command{} = OpMsg, _RequestId) ->
  <<
    ?put_header(?OpMsgOpcode),
    ?put_uint32(0), % Flags
    (put_section_type_zero(OpMsg#op_msg_command{database = make_bin(Db)}))/binary
  >>.

make_bin(Atom) when is_atom(Atom) ->
  erlang:atom_to_binary(Atom, utf8);
make_bin(Bin) ->
  Bin.

put_section_type_zero(#op_msg_command{
  command_doc = Doc,
  database = Database
}) ->
  <<
    ?put_uint8(0),
    (bson_binary:put_document(bson:merge(Doc, {<<"$db">>, Database})))/binary
  >>;
put_section_type_zero(#op_msg_write_op{
  command = Command,
  collection = Collection,
  database = Database,
  extra_fields = ExtraFields,
  documents_name = DocumentsName,
  documents = Documents
}) ->
  Msg = [
    {erlang:atom_to_binary(Command), Collection},
    {<<"$db">>, Database}
  ] ++
    ExtraFields
    ++
    [{DocumentsName, Documents}],
  <<
    ?put_uint8(0),
    (bson_binary:put_document(bson:document(Msg)))/binary
  >>.

-spec get_reply(binary()) -> {requestid(), reply(), binary()}.
get_reply(<<?get_header(?ReplyOpcode, _), _/binary>> = Message) ->
  <<?get_header_ignore_req_id(?ReplyOpcode, ResponseTo),
  ?get_bits32(_, _, _, _, AwaitCapable, _, QueryError, CursorNotFound),
  ?get_int64(CursorId),
  ?get_int32(StartingFrom),
  ?get_int32(NumDocs),
  Bin/binary>> = Message,
  {Docs, BinRest} = get_docs(NumDocs, Bin, []),
  Reply = #reply{
    cursornotfound = bool(CursorNotFound),
    queryerror = bool(QueryError),
    awaitcapable = bool(AwaitCapable),
    cursorid = CursorId,
    startingfrom = StartingFrom,
    documents = Docs
  },
  {ResponseTo, Reply, BinRest};
get_reply(<<?get_header(?OpMsgOpcode, _), _/binary>> = Message) ->
  <<?get_int32(_RequestId_),
    ?get_int32(ResponseTo),
    ?get_uint32(?OpMsgOpcode),
    ?get_uint32(_Flags),
    Bin1/binary>> = Message,
  %% For now assume the sequence type is zero
  <<?get_uint8(0), % Sequence type
    Bin2/binary>> = Bin1,
  {Doc, Rest} = bson_binary:get_map(Bin2),
  Reply = #op_msg_response{
    response_doc = Doc
  },
  {ResponseTo, Reply, Rest}.

-spec binarize(binary() | atom()) -> binary().
%@doc Ensures the given term is converted to a UTF-8 binary.
binarize(Term) when is_binary(Term) -> Term;
binarize(Term) when is_atom(Term) -> atom_to_binary(Term, utf8).

%% @private
get_docs(0, Bin, Docs) -> {lists:reverse(Docs), Bin};
get_docs(NumDocs, Bin, Docs) when NumDocs > 0 ->
  {Doc, Bin1} = bson_binary:get_map(Bin),
  get_docs(NumDocs - 1, Bin1, [Doc | Docs]).

%% @private
bit(false) -> 0;
bit(true) -> 1.

%% @private
bool(0) -> false;
bool(1) -> true.

%% @private
add_proj(Projector) when is_map(Projector) ->
  case map_size(Projector) of
    0 -> <<>>;
    _ -> bson_binary:put_document(Projector)
  end;
add_proj(Other) -> bson_binary:put_document(Other).
