%%%
%%%   Copyright (c) 2014-2020, Klarna AB
%%%
%%%   Licensed under the Apache License, Version 2.0 (the "License");
%%%   you may not use this file except in compliance with the License.
%%%   You may obtain a copy of the License at
%%%
%%%       http://www.apache.org/licenses/LICENSE-2.0
%%%
%%%   Unless required by applicable law or agreed to in writing, software
%%%   distributed under the License is distributed on an "AS IS" BASIS,
%%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%%   See the License for the specific language governing permissions and
%%%   limitations under the License.
%%%

-module(kpro_connection).

%% API
-export([ all_cfg_keys/0
        , get_api_vsns/1
        , get_endpoint/1
        , get_tcp_sock/1
        , init/4
        , loop/2
        , request_sync/3
        , request_async/2
        , send/2
        , start/3
        , stop/1
        , debug/2
        ]).

%% system calls support for worker process
-export([ system_continue/3
        , system_terminate/4
        , system_code_change/4
        , format_status/2
        ]).

-export_type([ config/0
             , connection/0
             ]).

-include("kpro_private.hrl").

-define(DEFAULT_REQUEST_TIMEOUT, timer:minutes(4)).
-define(SIZE_HEAD_BYTES, 4).

-type cfg_key() :: debug
                 | nolink
                 | request_timeout
                 | kpro_socket:cfg_key().

-type cfg_val() :: term().
-type config() :: [{cfg_key(), cfg_val()}] | #{cfg_key() => cfg_val()}.
-type hostname() :: kpro:hostname().
-type portnum()  :: kpro:portnum().
-type connection() :: pid().

-define(undef, undefined).

-record(state, { parent :: pid()
               , req_timeout :: ?undef | timeout()
               , socket :: kpro_socket:socket()
               }).

%%%_* API ======================================================================

%% @doc Return all config keys make client config management easy.
-spec all_cfg_keys() -> [cfg_key()].
all_cfg_keys() ->
  [request_timeout, debug, nolink  | kpro_socket:all_cfg_keys()].

%% @doc Connect to the given endpoint.
%% The started connection pid is linked to caller
%% unless `nolink := true' is found in `Config'
-spec start(hostname(), portnum(), config()) -> {ok, pid()} | {error, any()}.
start(Host, Port, Config) when is_list(Config) ->
  start(Host, Port, maps:from_list(Config));
start(Host, Port, #{nolink := true} = Config) ->
  proc_lib:start(?MODULE, init, [self(), host(Host), Port, Config]);
start(Host, Port, Config) ->
  proc_lib:start_link(?MODULE, init, [self(), host(Host), Port, Config]).

%% @doc Same as @link request_async/2.
%% Only that the message towards connection process is a cast (not a call).
%% Always return 'ok'.
-spec send(connection(), kpro:req()) -> ok.
send(Pid, Request) ->
  erlang:send(Pid, {{self(), noreply}, {send, Request}}),
  ok.

%% @doc Send a request. Caller should expect to receive a response
%% having `Rsp#kpro_rsp.ref' the same as `Request#kpro_req.ref'
%% unless `Request#kpro_req.no_ack' is set to 'true'
-spec request_async(connection(), kpro:req()) -> ok | {error, any()}.
request_async(Pid, Request) ->
  call(Pid, {send, Request}).

%% @doc Send a request and wait for response for at most Timeout milliseconds.
-spec request_sync(connection(), kpro:req(), timeout()) ->
        ok | {ok, kpro:rsp()} | {error, any()}.
request_sync(Pid, Request, Timeout) ->
  case request_async(Pid, Request) of
    ok when Request#kpro_req.no_ack ->
      ok;
    ok ->
      wait_for_rsp(Pid, Request, Timeout);
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc Stop socket process.
-spec stop(connection()) -> ok | {error, any()}.
stop(Pid) when is_pid(Pid) ->
  call(Pid, stop);
stop(_) ->
  ok.

-spec get_api_vsns(pid()) ->
        {ok, ?undef | kpro:vsn_ranges()} | {error, any()}.
get_api_vsns(Pid) ->
  call(Pid, get_api_vsns).

-spec get_endpoint(pid()) -> {ok, kpro:endpoint()} | {error, any()}.
get_endpoint(Pid) ->
  call(Pid, get_endpoint).

%% @hidden Test only
-spec get_tcp_sock(pid()) -> {ok, gen_tcp:socket()}.
get_tcp_sock(Pid) ->
  call(Pid, get_tcp_sock).

%% @doc Enable/disable debugging on the socket process.
%%      debug(Pid, pring) prints debug info on stdout
%%      debug(Pid, File) prints debug info into a File
%%      debug(Pid, none) stops debugging
-spec debug(connection(), print | string() | none) -> ok.
debug(Pid, none) ->
  system_call(Pid, {debug, no_debug});
debug(Pid, print) ->
  system_call(Pid, {debug, {trace, true}});
debug(Pid, File) when is_list(File) ->
  system_call(Pid, {debug, {log_to_file, File}}).

%%%_* Internal functions =======================================================

-spec init(pid(), hostname(), portnum(), config()) -> no_return().
init(Parent, Host, Port, Config) ->
  State =
    try
      Socket = kpro_socket:connect(Host, Port, Config),
      ReqTimeout = get_request_timeout(Config),
      ok = send_assert_max_req_age(self(), ReqTimeout),
      #state{ parent = Parent
            , req_timeout = ReqTimeout
            , socket = Socket
            }
    catch
      error : Reason ?BIND_STACKTRACE(Stack) ->
        proc_lib:init_ack(Parent, {error, {Reason, Stack}}),
        erlang:exit(normal)
    end,
  Debug = sys:debug_options(maps:get(debug, Config, [])),
  proc_lib:init_ack(Parent, {ok, self()}),
  loop(State, Debug).

-spec wait_for_rsp(connection(), kpro:req(), timeout()) ->
        {ok, term()} | {error, any()}.
wait_for_rsp(Pid, #kpro_req{ref = Ref}, Timeout) ->
  Mref = erlang:monitor(process, Pid),
  receive
    {msg, Pid, #kpro_rsp{ref = Ref} = Rsp} ->
      erlang:demonitor(Mref, [flush]),
      {ok, Rsp};
    {'DOWN', Mref, _, _, Reason} ->
      {error, {connection_down, Reason}}
  after
    Timeout ->
      erlang:demonitor(Mref, [flush]),
      {error, timeout}
  end.

system_call(Pid, Request) ->
  Mref = erlang:monitor(process, Pid),
  erlang:send(Pid, {system, {self(), Mref}, Request}),
  receive
    {Mref, Reply} ->
      erlang:demonitor(Mref, [flush]),
      Reply;
    {'DOWN', Mref, _, _, Reason} ->
      {error, {connection_down, Reason}}
  end.

call(Pid, Request) ->
  Mref = erlang:monitor(process, Pid),
  erlang:send(Pid, {{self(), Mref}, Request}),
  receive
    {Mref, Reply} ->
      erlang:demonitor(Mref, [flush]),
      Reply;
    {'DOWN', Mref, _, _, Reason} ->
      {error, {connection_down, Reason}}
  end.

-spec maybe_reply({pid(), reference() | noreply}, term()) -> ok.
maybe_reply({_, noreply}, _) ->
  ok;
maybe_reply({To, Ref}, Reply) ->
  _ = erlang:send(To, {Ref, Reply}),
  ok.

loop(#state{} = State, Debug) ->
  Msg = receive Input -> Input end,
  decode_msg(Msg, State, Debug).

decode_msg({system, From, Msg}, #state{parent = Parent} = State, Debug) ->
  sys:handle_system_msg(Msg, From, Parent, ?MODULE, Debug, State);
decode_msg(Msg, State, [] = Debug) ->
  handle_msg(Msg, State, Debug);
decode_msg(Msg, State, Debug0) ->
  Debug = sys:handle_debug(Debug0, fun print_msg/3, State, Msg),
  handle_msg(Msg, State, Debug).

handle_msg({T, Sock, Bin}, #state{socket = Socket0} = State, Debug)
 when (T =:= tcp orelse T =:= ssl) andalso is_binary(Bin) ->
  {Socket, Caller, Rsp} = kpro_socket:handle_response(Socket0, Sock, Bin),
  ok = cast(Caller, {msg, self(), Rsp}),
  ?MODULE:loop(State#state{socket = Socket}, Debug);
handle_msg(assert_max_req_age, #state{ socket = Socket
                                     , req_timeout = ReqTimeout
                                     } = State, Debug) ->
  Self = self(),
  erlang:spawn_link(fun() ->
                        ok = assert_max_req_age(Socket, ReqTimeout),
                        ok = send_assert_max_req_age(Self, ReqTimeout)
                    end),
  ?MODULE:loop(State, Debug);
handle_msg({tcp_closed, _Sock}, _, _) ->
  exit({shutdown, tcp_closed});
handle_msg({ssl_closed, _Sock}, _, _) ->
  exit({shutdown, ssl_closed});
handle_msg({tcp_error, _Sock, Reason}, _, _) ->
  exit({tcp_error, Reason});
handle_msg({ssl_error, _Sock, Reason}, _, _) ->
  exit({ssl_error, Reason});
handle_msg({From, {send, Request}},
           #state{socket = Socket0} = State, Debug) ->
  {Caller, _Ref} = From,
  Socket = kpro_socket:send(Socket0, Caller, Request),
  maybe_reply(From, ok),
  ?MODULE:loop(State#state{socket = Socket}, Debug);
handle_msg({From, get_api_vsns}, State, Debug) ->
  maybe_reply(From, {ok, kpro_socket:get_api_vsns(State#state.socket)}),
  ?MODULE:loop(State, Debug);
handle_msg({From, get_endpoint}, State, Debug) ->
  maybe_reply(From, {ok, kpro_socket:get_remote(State#state.socket)}),
  ?MODULE:loop(State, Debug);
handle_msg({From, get_tcp_sock}, State, Debug) ->
  maybe_reply(From, {ok, kpro_socket:get_tcp_sock(State#state.socket)}),
  ?MODULE:loop(State, Debug);
handle_msg({From, stop}, #state{socket = Socket}, _Debug) ->
  kpro_socket:close(Socket),
  maybe_reply(From, ok),
  ok;
handle_msg(Msg, #state{} = State, Debug) ->
  error_logger:warning_msg("[~p] ~p got unrecognized message: ~p",
                          [?MODULE, self(), Msg]),
  ?MODULE:loop(State, Debug).

cast(Pid, Msg) ->
  try
    Pid ! Msg,
    ok
  catch _ : _ ->
    ok
  end.

system_continue(_Parent, Debug, State) ->
  ?MODULE:loop(State, Debug).

-spec system_terminate(any(), _, _, _) -> no_return().
system_terminate(Reason, _Parent, Debug, _Misc) ->
  sys:print_log(Debug),
  exit(Reason).

system_code_change(State, _Module, _Vsn, _Extra) ->
  {ok, State}.

format_status(Opt, Status) ->
  {Opt, Status}.

print_msg(Device, {_From, {send, Request}}, _State) ->
  do_print_msg(Device, "send: ~p", [Request]);
print_msg(Device, {tcp, _Sock, Bin}, _State) ->
  do_print_msg(Device, "tcp: ~p", [Bin]);
print_msg(Device, {tcp_closed, _Sock}, _State) ->
  do_print_msg(Device, "tcp_closed", []);
print_msg(Device, {tcp_error, _Sock, Reason}, _State) ->
  do_print_msg(Device, "tcp_error: ~p", [Reason]);
print_msg(Device, {_From, stop}, _State) ->
  do_print_msg(Device, "stop", []);
print_msg(Device, Msg, _State) ->
  do_print_msg(Device, "unknown msg: ~p", [Msg]).

do_print_msg(Device, Fmt, Args) ->
  io:format(Device, "[~s] ~p " ++ Fmt ++ "~n", [ts(), self() | Args]).

ts() ->
  Now = os:timestamp(),
  {_, _, MicroSec} = Now,
  {{Y, M, D}, {HH, MM, SS}} = calendar:now_to_local_time(Now),
  lists:flatten(io_lib:format("~.4.0w-~.2.0w-~.2.0w ~.2.0w:~.2.0w:~.2.0w.~w",
                              [Y, M, D, HH, MM, SS, MicroSec])).

%% Get request timeout from config.
-spec get_request_timeout(config()) -> timeout().
get_request_timeout(Config) ->
  maps:get(request_timeout, Config, ?DEFAULT_REQUEST_TIMEOUT).

-spec assert_max_req_age(kpro_socket:socket(), timeout()) -> ok | no_return().
assert_max_req_age(Socket, Timeout) ->
  case kpro_socket:max_req_age(Socket) of
    Age when Age > Timeout ->
      erlang:exit(request_timeout);
    _ ->
      ok
  end.

%% Send the 'assert_max_req_age' message to connection process.
%% The send interval is set to a half of configured timeout.
-spec send_assert_max_req_age(connection(), timeout()) -> ok.
send_assert_max_req_age(Pid, Timeout) when Timeout >= 1000 ->
  %% Check every 1 minute
  %% or every half of the timeout value if it's less than 2 minute
  SendAfter = erlang:min(Timeout div 2, timer:minutes(1)),
  _ = erlang:send_after(SendAfter, Pid, assert_max_req_age),
  ok.

%% Allow binary() host name.
host(Host) when is_binary(Host) -> binary_to_list(Host);
host(Host) -> Host.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
