-module(kpro_oauth_tests).

-include_lib("eunit/include/eunit.hrl").

run_test_() ->
    {timeout, 30, fun do/0}.

do() ->
    Host = "bootstrap.msg-stream.connect-business.net",
    Port = 443,
    Config = [{ssl, [{cacertfile, "/tmp/ca.pem"},{log_level, debug}]}],
    {ok, _Pid} = kpro_connection:start(Host, Port, Config).

