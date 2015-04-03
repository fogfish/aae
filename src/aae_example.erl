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
%%   anti-entropy protocol example 
-module(aae_example).
-behaviour(aae).

-include("aae.hrl").

-export([
   new/1
  ,peers/1
  ,session/2
  ,handshake/3
  ,chunks/1
  ,snapshot/1
  ,snapshot/2
  ,diff/4
]).
-export([run/0, run/1]).

%%
%% data types
-type(state() :: map()).
-type(peer()  :: pid()).
-type(key()   :: any()).
-type(val()   :: any()).

%%
%% initialize anti-entropy leader state
%% return identity of itself and new state data 
-spec(new/1 :: (list()) -> {any(), state()}).

new(_) ->
   random:seed(erlang:now()),
   pg2:join(?MODULE, self()),
   {self(), #{}}.

%%
%% return list of candidate peers 
-spec(peers/1 :: (state()) -> {[peer()], state()}).

peers(State) ->
   {[X || X <- pg2:get_members(?MODULE), X =/= self()], State}.


%%
%% initialize new anti-entropy session
%%
-spec(session/2 :: (peer(), state()) -> state()).

session(_Peer, State) ->
   random:seed(erlang:now()),
   State.

%%
%% connect gossip session to selected remote peer using pipe protocol
-spec(handshake/3 :: (peer(), any(), state()) -> state()).

handshake(Peer, Req, State) ->
   pipe:send(Peer, Req),
   State.

%%
%% return chunk(s) to apply anti-entropy
-spec(chunks/1 :: (any()) -> any()).

chunks(State) ->
   {random:uniform(10), State}.

%%
%% make snapshot, returns key/val stream 
-spec(snapshot/1 :: (any()) -> datum:stream()).
-spec(snapshot/2 :: (list(), any()) -> datum:stream()).

snapshot(State) ->
   Stream = stream:build(
      lists:usort(
         [{random:uniform(X), random:uniform(3)} || X <- lists:seq(1, 100)]
      )
   ),
   {Stream, State}.

snapshot(Chunk, State) ->
   Stream = stream:build(
      lists:usort(
         [{random:uniform(Chunk * 100), random:uniform(3)} || _ <- lists:seq(1, Chunk)]
      )
   ),
   {Stream, State}.

%%
%% remote peer diff, called for each key, order is arbitrary 
-spec(diff/4 :: (peer(), key(), val(), state()) -> ok).

diff(_Peer, _Key, _Val, _State) ->
   ?DEBUG("==> ~p ~p (~p)~n", [self(), _Key, _Val]).


%%%----------------------------------------------------------------------------   
%%%
%%% gossip example
%%%
%%%----------------------------------------------------------------------------   

-define(SPEC, [
   {session, {10000, 0.5}}
  ,{timeout,        10000}
  ,{capacity,          10}
  ,{strategy,       chunk}
  ,{adapter, {?MODULE, []}}
]).

run() ->
   run(2).

run(N) ->
   aae:start(),
   lager:set_loglevel(lager_console_backend, debug),
   pg2:create(?MODULE),
   lists:foreach(
      fun(_) ->
         {ok, Pid} = aae:start_link(?SPEC),
         pg2:join(?MODULE, Pid)
      end,
      lists:seq(1, N)
   ).
