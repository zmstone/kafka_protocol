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
-module(kpro_socket_tests).

-include_lib("eunit/include/eunit.hrl").

api_vsn_test() ->
  Proto = plaintext,
  ConnConfig = kpro_test_lib:connection_config(Proto),
  Endpoints = kpro_test_lib:get_endpoints(Proto),
  Socket = kpro:connect_socket(hd(Endpoints), ConnConfig),
  {ok, Versions} = kpro:get_api_versions(Socket),
  ?assert(is_map(Versions)),
  kpro:close_socket(Socket).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
