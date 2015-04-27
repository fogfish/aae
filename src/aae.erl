%%
%%   Copyright (c) 2012, Dmitry Kolesnikov
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

-export([start/0]).
-export([behaviour_info/1]).
-export([
   start_link/1
  ,start_link/2
  ,run/2
  ,i/0
]).

%%
%% RnD application start
start() ->
   applib:boot(?MODULE, []).

%%
%% 
behaviour_info(callbacks) ->
   [
      %%
      %% initialize anti-entropy leader state
      %% return identity of itself and new state data 
      %%
      %% -spec(new/1 :: (list()) -> {any(), state()}).
      {new, 1}

      %%
      %% return list of candidate peers 
      %%
      %% -spec(peers/1 :: (state()) -> {[peer()], state()}).
     ,{peers, 1}

      %%
      %% initialize new anti-entropy session
      %%
      %% -spec(session/1 :: (peer(), state()) -> state()).
     ,{session, 2}

      %%
      %% connect session to selected remote peer using pipe protocol
      %% 
      %% -spec(handshake/3 :: (peer(), any(), state()) -> state()).
     ,{handshake, 3}

      %%
      %% make snapshot, returns key/val stream 
      %%
      %% -spec(snapshot/1 :: (state()) -> {datum:stream(), state()}).
     ,{snapshot,  1}

      %%
      %% remote peer diff, called for each key, order is arbitrary 
      %%
      %% -spec(diff/4 :: (peer(), key(), state()) -> ok).
     ,{diff, 3}
   ];
behaviour_info(_) ->
   undefined.
 
%%
%% start instance of active anti-entropy
%%  Options
%%    {session,  timeout()} - timeout to establish session, infinity implies a manual trigger
%%    {timeout,  timeout()} - peer i/o timeout
%%    {capacity, integer()} - max number of simultaneous sessions
%%    {strategy,       aae} - reconciliation strategy
%%    {adapter, {atom(), any()}} - aae behavior
start_link(Opts) ->
   aae_leader:start_link(Opts).

start_link(Name, Opts) ->
   aae_leader:start_link(Name, Opts).

%%
%% force anti-entropy session
run(Pid, Peer) ->
   pipe:call(Pid, {run, Peer}).

%%
%% list all active session
i() ->
   [Pid || {_, Pid, _, _} <- supervisor:which_children(aae_session_sup)].

