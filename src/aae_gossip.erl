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
-module(aae_gossip).
-behaviour(pipe).

-include("aae.hrl").

-export([
   start_link/1
  ,init/1
  ,free/2
  ,ioctl/2
  ,idle/3
  ,exchange/3
]).

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

start_link(Opts) ->
   pipe:start_link(?MODULE, [Opts], []).

init([Opts]) ->
   lager:md([{lib, aae}]),
   random:seed(os:timestamp()),
   {ok, idle, lists:foldl(fun init/2, #{}, Opts)}.

init({timeout, X}, State) ->
   State#{t => X};
init({adapter, X}, State) ->
   State#{adapter => X};
init(_, State) ->
   State.

free(_, _) ->
   ok.

ioctl(_, _) ->
   throw(not_implemented).


%%%----------------------------------------------------------------------------   
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------   

%%%
%%% IDLE
%%%

idle({session, _Node, Peer}, Pipe, #{adapter := {Mod, Adapter0}} = State) ->
   ?INFO("aae : +gossip ~p", [Peer]),
   pipe:emit(Pipe, self(), exchange),
   {Stream, Adapter1} = Mod:snapshot(Adapter0),
   {next_state, exchange, 
      State#{
         peer    => Peer
        ,stream  => Stream
        ,adapter => {Mod, Adapter1}
      }
   }.

%%%
%%% EXCHANGE
%%%

exchange(exchange, _Pipe, #{peer := Peer, stream := Stream, adapter := {Mod, Adapter0}}=State) ->
   stream:foreach(
      fun({_Key, Val}) ->
         Mod:diff(Peer, Val, Adapter0)
      end,
      Stream
   ),
   {stop, normal, State}.

%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

