%%
%%   Copyright (c) 2012 - 2015, Dmitry Kolesnikov
%%   All Rights Reserved.
%%
%%   Licensed under the Apache License, Version 2.0 (the "License");
%%   you may not use this file except in compliance with the License.
%%   You may obtain a copy of the License at
%%
%%       http://www.apache.org/licenses/LICENSE-2.0
%%
%%   Unless required by applicable law or agreed to in writing, software
%%   distributed under the License is distributed on an "AS IS" BASIS,
%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%   See the License for the specific language governing permissions and
%%   limitations under the License.
%%
%% @description
%%   active anti-entropy
-module(aae).
-compile({parse_transform, category}).

-export([start/0]).
-export([behaviour_info/1]).
-export([
   topic/2
  ,gossip/3
  ,gossip/4
  ,i/1
]).

%%
%%
-type topic()  :: binary().
-type opts()   :: [{atom(), _}].
-type key()    :: _.
-type digest() :: _. 


%%
%% RnD application start
start() ->
   applib:boot(?MODULE, []).

%%
%% 
behaviour_info(callbacks) ->
   [
      %%
      %% return list of susceptible cluster nodes
      %%
      %% -spec nodes(_) -> [node()].
      {nodes, 1}

      %%
      %% return list of susceptible peers for given key
      %%
      %% -spec peers(key(), _) -> [node()].
     ,{peers, 2}

      %%
      %% return list of local susceptible processes for given key
      %%
      %% -spec processes(key(), _) -> [pid()].
     ,{processes, 2}

      %%
      %% compare values, return true if A =< B
      %%
      %% -spec descend(digest(), digest()) -> true | false.
     ,{descend, 2}

      %%
      %% compare values, return true if A = B
      %% -spec equal(digest(), digest()) -> true | false.
     ,{equal, 2}
   ];
behaviour_info(_) ->
   undefined.
 
%%
%% join peer to gossip *topic*
%%  Options:
%%   * {strategy, ...} -
%%   * {cycle,  integer()} - cycle gossip sessions
%%   * {packet, integer()} - size of gossip packet
-spec topic(topic(), opts()) -> {ok, pid()} | {error, _}.

topic(Topic, Opts) ->
   supervisor:start_child(aae_gossip_sup, [scalar:s(Topic), Opts]).

%%
%% 
-spec gossip(topic(), key(), digest()) -> ok | {error, _}.
-spec gossip(topic(), key(), digest(), timeout()) -> ok | {error, _}.

gossip(Topic, Key, Rumor) ->
   gossip(Topic, Key, Rumor, 5000).

gossip(Topic, Key, Rumor, Timeout) ->
   [$. ||
      scalar:s(Topic),
      pns:whereis(aae, _),
      pipe:call(_, {gossip, Key, Rumor}, Timeout)
   ].

%%
%%
-spec i(topic()) -> [_].

i(Topic) ->
   [$. ||
      scalar:s(Topic),
      pns:whereis(aae, _),
      pipe:ioctl(_, i)
   ].

