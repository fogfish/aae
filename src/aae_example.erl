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
%%   anti-entropy protocol example 
-module(aae_example).
% -behaviour(aae).

-include("aae.hrl").

-export([
   new/1
  ,free/2
  ,peers/1
  ,session/2
  ,handshake/3
  ,snapshot/1
  ,diff/3
]).
-export([run/0, run/1, run/2]).

%%
%% data types
-type(state() :: map()).
-type(peer()  :: pid()).
-type(val()   :: any()).

%%
%% initialize anti-entropy leader state
%% return identity of itself and new state data 
-spec new(list()) -> {any(), state()}.

new(_) ->
   pg2:join(?MODULE, self()),
   {self(), #{}}.

%%
%% terminate anti-entropy state either session or leader
-spec free(any(), state()) -> state().

free(_, _) ->
   ok.


%%
%% return list of candidate peers 
-spec peers(state()) -> {[peer()], state()}.

peers(State) ->
   {[X || X <- pg2:get_members(?MODULE), X =/= self()], State}.


%%
%% initialize new anti-entropy session
%%
-spec session(peer(), state()) -> state().

session(_Peer, State) ->
   State.

%%
%% connect gossip session to selected remote peer using pipe protocol
-spec handshake(peer(), any(), state()) -> state().

handshake(Peer, Req, State) ->
   pipe:send(Peer, Req),
   State.

%%
%% make snapshot, returns key/val stream 
-spec snapshot(any()) -> datum:stream().

snapshot(State) ->
   Stream = stream:build(
      lists:usort(
         [rand:uniform(X) || X <- lists:seq(1, 100)]
      )
   ),
   {Stream, State}.

%%
%% remote peer diff, called for each key, order is arbitrary 
-spec diff(peer(), val(), state()) -> ok.

diff(_Peer, _Key, _State) ->
   ?DEBUG("==> ~p ~p~n", [self(), _Key]).


%%%----------------------------------------------------------------------------   
%%%
%%% gossip example
%%%
%%%----------------------------------------------------------------------------   

-define(SPEC(X), [
   {session, {10000, 0.5}}
  ,{timeout,        10000}
  ,{strategy,           X}
  ,{capacity,          10}
  ,{adapter, {?MODULE, []}}
]).

run() ->
   run(aae, 2).

run(Strategy) ->
   run(Strategy, 2).

run(Strategy, N) ->
   aae:start(),
   lager:set_loglevel(lager_console_backend, debug),
   pg2:create(?MODULE),
   lists:foreach(
      fun(_) ->
         {ok, Pid} = aae:start_link(?SPEC(Strategy)),
         pg2:join(?MODULE, Pid)
      end,
      lists:seq(1, N)
   ).

