%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 13. Apr 2015 9:47
%%%-------------------------------------------------------------------
-module(auth_test).
-author("tihon").

-define(ANS_1_EXPECTED, <<"c=biws,r=fyko+d2lbbFgONRv9qkxdawLHo+Vgk7qvUOKUwuWLIWg4l/9SraGMHEE">>).

-include("mongo_protocol.hrl").
-include_lib("eunit/include/eunit.hrl").

%% https://github.com/mongodb/specifications/blob/master/source/auth/auth.rst#scram-sha-1
auth_test() ->
  User = <<"user">>,
  Password = <<"pencil">>,
  Random = <<"fyko+d2lbbFgONRv9qkxdawL">>,
  FirstMessage = mc_auth_logic:compose_first_message(User, Random),
  Message = <<?GS2_HEADER/binary, FirstMessage/binary>>,
  ?assertEqual(<<"n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL">>, Message),

  FirstResponce = <<"r=fyko+d2lbbFgONRv9qkxdawLHo+Vgk7qvUOKUwuWLIWg4l/9SraGMHEE,s=rQ9ZY3MntBeuP3E1TDVC4w==,i=10000">>,
  SecondMessage = mc_auth_logic:compose_second_message(FirstResponce, User, Password, Random, FirstMessage),
  ?debugFmt("Exp ~p~nGot ~p", [?ANS_1_EXPECTED, SecondMessage]),
  ?assertEqual(?ANS_1_EXPECTED, SecondMessage),

  ?assert(true).

