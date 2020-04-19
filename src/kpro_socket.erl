%%%
%%%   Copyright (c) 2020, Klarna AB
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

%% This module manages a gen_tcp:socket() in an opaque state which can be
%% embedded to producer or consumer processes for direct access of the socket
%% instead of going though `kpro_connection' process.
%% This can be useful in cases where the connection is already exclusively owned
%% by a producer or consumer (i.e. not shared between producers or consumers)
%% in order to cut the overhead of copying requests between processes.
-module(kpro_socket).

-export([ all_cfg_keys/0
        , close/1
        , connect/3
        , get_api_vsns/1
        , get_remote/1
        , get_tcp_sock/1
        , max_req_age/1
        , send/3
        , handle_response/3
        ]).

-export_type([socket/0]).

-include("kpro_private.hrl").

-define(undef, undefined).
-define(DEFAULT_CONNECT_TIMEOUT, timer:seconds(5)).

-type client_id() :: kpro:client_id().
-type requests() :: kpro_sent_reqs:requests().
-type cfg_key() :: connect_timeout
                 | client_id
                 | extra_sock_opts
                 | query_api_versions
                 | sasl
                 | ssl.

-type cfg_val() :: term().
-type config() :: [{cfg_key(), cfg_val()}] | #{cfg_key() => cfg_val()}.
-type hostname() :: kpro:hostname().
-type portnum()  :: kpro:portnum().
-record(socket, { client_id   :: client_id()
                , remote      :: kpro:endpoint()
                , sock        :: gen_tcp:socket() | ssl:sslsocket()
                , mod         :: ?undef | gen_tcp | ssl
                , api_vsns    :: ?undef | kpro:vsn_ranges()
                , requests    :: ?undef | requests()
                }).

-opaque socket() :: #socket{}.

%% @doc Return all config keys make client config management easy.
-spec all_cfg_keys() -> [cfg_key()].
all_cfg_keys() ->
  [connect_timeout, client_id, sasl, ssl, query_api_versions, extra_sock_opts].

%% @doc Connect to the given endpoint, then initalize connection.
%% Raise an error exception for any failure.
-spec connect(hostname(), portnum(), config()) -> socket().
connect(Host, Port, Config) ->
  try
    Socket = do_connect(Host, Port, Config),
    %% From now on, enter `{active, once}' mode
    %% NOTE: ssl doesn't support `{active, N}'
    ok = setopts(Socket#socket.sock, Socket#socket.mod, [{active, once}]),
    Socket
  catch
    error : Reason ?BIND_STACKTRACE(Stack) ->
      ?GET_STACKTRACE(Stack),
      IsSsl = maps:get(ssl, Config, false),
      SaslOpt = get_sasl_opt(Config),
      ok = maybe_log_hint(Host, Port, Reason, IsSsl, SaslOpt),
      erlang:raise(error, Reason, Stack)
  end.

%% @doc Get the age in milliseconds of the oldest sent request which is
%% still pending for a response.
-spec max_req_age(socket()) -> timeout().
max_req_age(#socket{requests = Requests}) ->
  kpro_sent_reqs:scan_for_max_age(Requests).

%% @doc Send an async request and keep its reference to match future response.
-spec send(socket(), pid(), kpro:req()) -> socket().
send(Socket, Caller, Request) ->
  #socket{ mod = Mod
         , sock = Sock
         , requests = Requests
         , client_id = ClientId
         } = Socket,
  #kpro_req{api = API, vsn = Vsn} = Request,
  {CorrId, NewRequests} =
    case Request of
      #kpro_req{no_ack = true} ->
        kpro_sent_reqs:increment_corr_id(Requests);
      #kpro_req{ref = Ref} ->
        kpro_sent_reqs:add(Requests, Caller, Ref, API, Vsn)
    end,
  RequestIoData = kpro_req_lib:encode(ClientId, CorrId, Request),
  Res = Mod:send(Sock, RequestIoData),
  case Res of
    ok ->
      Socket#socket{requests = NewRequests};
    {error, Reason0} ->
      Reason = [ {api, API}
               , {vsn, Vsn}
               , {caller, Caller}
               , {reason, Reason0}
               ],
      exit({send_error, Reason})
  end.

%% @doc Handle response `{tcp | ssl, gen_tcp:socket(), binary()}`
%% received from gen_tcp socket.
-spec handle_response(socket(), gen_tcp:socket(), binary()) ->
        {socket(), pid(), kpro:rsp()}.
handle_response(#socket{ sock = Sock
                       , mod = Mod
                       , requests = Requests
                       } = Socket, Sock, Data) ->
  ok = setopts(Sock, Mod, [{active, once}]),
  {CorrId, Body} = kpro_lib:decode_corr_id(Data),
  {Caller, Ref, API, Vsn} = kpro_sent_reqs:get_req(Requests, CorrId),
  Rsp = kpro_rsp_lib:decode(API, Vsn, Body, Ref),
  NewRequests = kpro_sent_reqs:del(Requests, CorrId),
  {Socket#socket{requests = NewRequests}, Caller, Rsp}.

%% @doc Close socket.
-spec close(socket()) -> _.
close(#socket{sock = Sock, mod = Mod}) ->
  Mod:close(Sock).

%% @doc Get api versions.
-spec get_api_vsns(socket()) -> {ok, ?undef | kpro:vsn_ranges()}.
get_api_vsns(#socket{api_vsns = Vsns}) -> {ok, Vsns}.

%% @doc Get the endpoint it is connected to.
-spec get_remote(socket()) -> kpro:endpoint().
get_remote(#socket{remote = Remote}) -> Remote.

%% @hidden Get tpc socket.
-spec get_tcp_sock(pid()) -> gen_tcp:socket().
get_tcp_sock(#socket{sock = Sock}) -> Sock.

%%%_* Internal functions =======================================================

-spec do_connect(hostname(), portnum(), config()) -> socket().
do_connect(Host, Port, Config) ->
  Timeout = get_connect_timeout(Config),
  %% initial active opt should be 'false' before upgrading to ssl
  SockOpts = [{active, false}, binary] ++ get_extra_sock_opts(Config),
  case gen_tcp:connect(host(Host), Port, SockOpts, Timeout) of
    {ok, Sock} ->
      Socket = #socket{ client_id = get_client_id(Config)
                      , remote    = {Host, Port}
                      , sock      = Sock
                      , requests  = kpro_sent_reqs:new()
                      },
      init_connection(Socket, Config);
    {error, Reason} ->
      erlang:error(Reason)
  end.

%% Initialize connection.
%% * Upgrade to SSL
%% * SASL authentication
%% * Query API versions
init_connection(#socket{ client_id = ClientId
                       , sock = Sock
                       , remote = {Host, _}
                       } = Socket, Config) ->
  Timeout = get_connect_timeout(Config),
  %% adjusting buffer size as per recommendation at
  %% http://erlang.org/doc/man/inet.html#setopts-2
  %% idea is from github.com/epgsql/epgsql
  {ok, [{recbuf, RecBufSize}, {sndbuf, SndBufSize}]} =
    inet:getopts(Sock, [recbuf, sndbuf]),
  ok = inet:setopts(Sock, [{buffer, max(RecBufSize, SndBufSize)}]),
  SslOpts = maps:get(ssl, Config, false),
  Mod = get_tcp_mod(SslOpts),
  NewSock = maybe_upgrade_to_ssl(Sock, Mod, SslOpts, Host, Timeout),
  %% from now on, it's all packet-4 messages
  ok = setopts(NewSock, Mod, [{packet, 4}]),
  Versions =
    case Config of
      #{query_api_versions := false} -> ?undef;
      _ -> query_api_versions(NewSock, Mod, ClientId, Timeout)
    end,
  HandshakeVsn = case Versions of
                   #{sasl_handshake := {_, V}} -> V;
                   _ -> 0
                 end,
  SaslOpts = get_sasl_opt(Config),
  ok = kpro_sasl:auth(Host, NewSock, Mod, ClientId,
                      Timeout, SaslOpts, HandshakeVsn),
  Socket#socket{mod = Mod, sock = NewSock, api_vsns = Versions}.

query_api_versions(Sock, Mod, ClientId, Timeout) ->
  Req = kpro_req_lib:make(api_versions, 0, []),
  Rsp = kpro_lib:send_and_recv(Req, Sock, Mod, ClientId, Timeout),
  ErrorCode = find(error_code, Rsp),
  case ErrorCode =:= ?no_error of
    true ->
      Versions = find(api_versions, Rsp),
      F = fun(V, Acc) ->
          API = find(api_key, V),
          MinVsn = find(min_version, V),
          MaxVsn = find(max_version, V),
          case is_atom(API) of
            true ->
              %% known API for client
              Acc#{API => {MinVsn, MaxVsn}};
            false ->
              %% a broker-only (ClusterAction) API
              Acc
          end
      end,
      lists:foldl(F, #{}, Versions);
    false ->
      erlang:error({failed_to_query_api_versions, ErrorCode})
  end.

get_tcp_mod(_SslOpts = true)  -> ssl;
get_tcp_mod(_SslOpts = [_|_]) -> ssl;
get_tcp_mod(_)                -> gen_tcp.

%% If SslOpts contains {verify, verify_peer}, we insert
%% {server_name_indication, Host}. This is necessary as of OTP 20, to
%% ensure that peer verification is done against the correct host name
%% (otherwise the IP will be used, which is almost certainly
%% incorrect).
insert_server_name_indication(SslOpts, Host) ->
  VerifyOpt = proplists:get_value(verify, SslOpts),
  insert_server_name_indication(VerifyOpt, SslOpts, Host).

insert_server_name_indication(verify_peer, SslOpts, Host) ->
  case proplists:get_value(server_name_indication, SslOpts) of
    undefined ->
      %% insert {server_name_indication, Host} if not already present
      [{server_name_indication, ensure_string(Host)} | SslOpts];
    _ ->
      SslOpts
  end;

insert_server_name_indication(_, SslOpts, _) ->
  SslOpts.

%% inet:hostname() is atom() | string()
%% however sni() is only allowed to be string()
ensure_string(Host) when is_atom(Host) -> atom_to_list(Host);
ensure_string(Host) -> Host.

maybe_upgrade_to_ssl(Sock, _Mod = ssl, SslOpts0, Host, Timeout) ->
  SslOpts = case SslOpts0 of
              true -> [];
              [_|_] -> insert_server_name_indication(SslOpts0, Host)
            end,

  case ssl:connect(Sock, SslOpts, Timeout) of
    {ok, NewSock} -> NewSock;
    {error, Reason} -> erlang:error({failed_to_upgrade_to_ssl, Reason})
  end;
maybe_upgrade_to_ssl(Sock, _Mod, _SslOpts, _Host, _Timeout) ->
  Sock.

setopts(Sock, _Mod = gen_tcp, Opts) -> inet:setopts(Sock, Opts);
setopts(Sock, _Mod = ssl, Opts)     ->  ssl:setopts(Sock, Opts).

-spec get_connect_timeout(config()) -> timeout().
get_connect_timeout(Config) ->
  maps:get(connect_timeout, Config, ?DEFAULT_CONNECT_TIMEOUT).

%% Ensure binary client id
get_client_id(Config) ->
  ClientId = maps:get(client_id, Config, <<"kpro-client">>),
  case is_atom(ClientId) of
    true -> atom_to_binary(ClientId, utf8);
    false -> ClientId
  end.

get_extra_sock_opts(Config) ->
  maps:get(extra_sock_opts, Config, []).

%% So far supported endpoint is tuple {Hostname, Port}
%% which lacks of hint on which protocol to use.
%% It would be a bit nicer if we support endpoint formats like below:
%%    PLAINTEX://hostname:port
%%    SSL://hostname:port
%%    SASL_PLAINTEXT://hostname:port
%%    SASL_SSL://hostname:port
%% which may give some hint for early config validation before trying to
%% connect to the endpoint.
%%
%% However, even with the hint, it is still quite easy to misconfig and endup
%% with a clueless crash report.  Here we try to make a guess on what went
%% wrong in case there was an error during connection estabilishment.
maybe_log_hint(Host, Port, Reason, IsSsl, SaslOpt) ->
  case hint_msg(Reason, IsSsl, SaslOpt) of
    ?undef ->
      ok;
    Msg ->
      error_logger:error_msg("Failed to connect to ~s:~p\n~s\n",
                             [Host, Port, Msg])
  end.

hint_msg({failed_to_upgrade_to_ssl, R}, _IsSsl, SaslOpt) when R =:= closed;
                                                              R =:= timeout ->
  case SaslOpt of
    ?undef -> "Make sure connecting to a 'SSL://' listener";
    _      -> "Make sure connecting to 'SASL_SSL://' listener"
  end;
hint_msg({sasl_auth_error, 'IllegalSaslState'}, true, _SaslOpt) ->
  "Make sure connecting to 'SASL_SSL://' listener";
hint_msg({sasl_auth_error, 'IllegalSaslState'}, false, _SaslOpt) ->
  "Make sure connecting to 'SASL_PLAINTEXT://' listener";
hint_msg({sasl_auth_error, {badmatch, {error, enomem}}}, false, _SaslOpts) ->
  %% This happens when KAFKA is expecting SSL handshake
  %% but client started SASL handshake instead
  "Make sure 'ssl' option is in client config, \n"
  "or make sure connecting to 'SASL_PLAINTEXT://' listener";
hint_msg(_, _, _) ->
  %% Sorry, I have no clue, please read the crash log
  ?undef.

%% Get sasl options from connection config.
-spec get_sasl_opt(config()) -> cfg_val().
get_sasl_opt(Config) ->
  case maps:get(sasl, Config, ?undef) of
    {Mechanism, User, Pass0} when ?IS_PLAIN_OR_SCRAM(Mechanism) ->
      Pass = case is_function(Pass0) of
               true  -> Pass0();
               false -> Pass0
             end,
      {Mechanism, User, Pass};
    {Mechanism, File} when ?IS_PLAIN_OR_SCRAM(Mechanism) ->
      {User, Pass} = read_sasl_file(File),
      {Mechanism, User, Pass};
    Other ->
      Other
  end.

%% Read a regular file, assume it has two lines:
%% First line is the sasl-plain username
%% Second line is the password
-spec read_sasl_file(file:name_all()) -> {binary(), binary()}.
read_sasl_file(File) ->
  {ok, Bin} = file:read_file(File),
  Lines = binary:split(Bin, <<"\n">>, [global]),
  [User, Pass] = lists:filter(fun(Line) -> Line =/= <<>> end, Lines),
  {User, Pass}.

find(FieldName, Struct) -> kpro_lib:find(FieldName, Struct).

%% Allow binary() host name.
host(Host) when is_binary(Host) -> binary_to_list(Host);
host(Host) -> Host.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
