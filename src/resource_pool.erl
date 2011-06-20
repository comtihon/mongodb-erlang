%@doc A set of N resources handed out randomly, and recreated on expiration
-module (resource_pool).

-export_type ([factory/1, create/1, expire/1, is_expired/1]).
-export_type ([pool/1]).
-export ([new/2, get/1, close/1, is_closed/1]).

-type maybe(A) :: {A} | {}.
-type err_or(A) :: {ok, A} | {error, any()}.

-spec trans_error (fun (() -> err_or(A))) -> A. % IO throws any()
%@doc Convert error return to throw
trans_error (Act) -> case Act() of {ok, A} -> A; {error, Reason} -> throw (Reason) end.

-type factory(A) :: {any(), create(A), expire(A), is_expired(A)}.
% Object for creating, destroying, and checking resources of type A.
% any() is the input to create(A). kept separate so we can identify the factory when printed.
-type create(A) :: fun ((any()) -> err_or(A)). % IO
-type expire(A) :: fun ((A) -> ok). % IO
-type is_expired(A) :: fun ((A) -> boolean()). % IO

-opaque pool(A) :: {factory(A), mvar:mvar (array:array (maybe(A)))}.
% Pool of N resources of type A, created on demand, recreated on expiration, and handed out randomly

-spec new (factory(A), integer()) -> pool(A).
%@doc Create empty pool that will create and destroy resources using given factory and allow up to N resources at once
new (Factory, MaxSize) -> {Factory, mvar:new (array:new (MaxSize, [{fixed, false}, {default, {}}]))}.

-spec get (pool(A)) -> err_or(A). % IO
%@doc Return a random resource from the pool, creating one if necessary. Error if failed to create
get ({{Input,Create,_,IsExpired}, VResources}) ->
	New = fun (Array, I) -> Res = trans_error (fun () -> Create (Input) end), {array:set (I, {Res}, Array), Res} end,
	Check = fun (Array, I, Res) -> case IsExpired (Res) of true -> New (Array, I); false -> {Array, Res} end end,
	try mvar:modify (VResources, fun (Array) ->
		R = random:uniform (array:size (Array)) - 1,
		case array:get (R, Array) of
			{Res} -> Check (Array, R, Res);
			{} -> New (Array, R) end end)
	of Resource -> {ok, Resource}
	catch Reason -> {error, Reason} end.

-spec close (pool(_)) -> ok. % IO
%@doc Close pool and all its resources
close ({{_,_,Expire,_}, VResources}) ->
	mvar:with (VResources, fun (Array) ->
		array:map (fun (_I, MRes) -> case MRes of {Res} -> Expire (Res); {} -> ok end end, Array) end),
	mvar:terminate (VResources).

-spec is_closed (pool(_)) -> boolean(). % IO
%@doc Has pool been closed?
is_closed ({_, VResources}) -> mvar:is_terminated (VResources).
