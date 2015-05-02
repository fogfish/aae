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
%%   active anti-entropy - leader processes
-module(aae_leader).
-behaviour(pipe).

-include("aae.hrl").

-export([
   start_link/1
  ,start_link/2
  ,init/1
  ,free/2
  ,ioctl/2
  ,idle/3
  ,busy/3
   % api
  ,session/2
]).

-define(rnd(X), lists:nth(random:uniform(length(X)), X)).

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

%%
%%
start_link(Opts) ->
   pipe:start_link(?MODULE, [Opts], []).

start_link(Name, Opts) ->
   pipe:start_link(Name, ?MODULE, [Opts], []).

%%
%%
init([Opts]) ->
   lager:md([{lib, aae}]),
   random:seed(os:timestamp()),
   {ok, idle, lists:foldl(fun init/2, #{strategy => aae, opts => Opts}, Opts)}.

init({session, X}, State) ->
   State#{t => tempus:timer(X, timeout)};

init({adapter, {Mod, Args}}, #{opts := Opts0}=State) ->
   {value, _, Opts1} = lists:keytake(adapter, 1, Opts0),
   {Node,   Adapter} = Mod:new(Args),
   State#{node => Node, adapter => {Mod, Adapter}, opts => Opts1};

init({strategy, X}, State) ->
   State#{strategy => X};

init(_, State) ->
   State.

%%
%%
free(_, _) ->
   ok.

%%
%%
ioctl(_, _) ->
   throw(not_implemented).

%%%----------------------------------------------------------------------------   
%%%
%%% api
%%%
%%%----------------------------------------------------------------------------   

%%
%% run new session
session(Pid, Peer) ->
   pipe:call(Pid, {run, Peer}).

%%%----------------------------------------------------------------------------   
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------   

%%
%%
idle(timeout, _Pipe, #{node := _Node, adapter := {Mod, Adapter0}}=State) ->
   case
      {is_allowed(State), Mod:peers(Adapter0)}
   of
      {true, {[],    Adapter1}} ->
         ?DEBUG("aae lead : ~p no peers", [_Node]),
         {next_state, idle, set_timeout(State#{adapter => {Mod, Adapter1}})};

      {true, {Peers, Adapter1}} ->
         aae:run(self(), ?rnd(Peers)),
         {next_state, idle, set_timeout(State#{adapter => {Mod, Adapter1}})};

      {false, _} ->
         ?DEBUG("aae lead : ~p no capacity", [_Node]),
         {next_state, idle, set_timeout(State)}
   end;

idle({run, Peer}, Pipe, #{node := _Node}=State) ->
   case is_allowed(State) of
      true  ->
         pipe:ack(Pipe, ok),
         {next_state, busy, connect(Peer, State)};            

      false ->
         pipe:ack(Pipe, {error, ebusy}),
         {next_state, idle, State}
   end;

idle({session, _} = Req, Pipe, #{node := _Node, t := T} = State) ->
   case is_allowed(State) of
      true  ->
         {next_state, busy, accept(Req, Pipe, State#{t => tempus:cancel(T)})};

      false ->
         ?DEBUG("aae lead : ~p no capacity", [_Node]),
         pipe:a(Pipe, {session, nok}),
         {next_state, idle, State}
   end.

%%
%%
busy({run, _Peer}, Pipe, State) ->
   pipe:ack(Pipe, {error, ebusy}),
   {next_state, busy, State};
   
busy({session, _}, Pipe, State) ->
   pipe:a(Pipe, {session, nok}),
   {next_state, busy, State};

busy({'DOWN', _, _Type, _Pid, _Reason}, _Pipe, State) ->
   {next_state, idle, set_timeout(State)}.


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%%
set_timeout(#{t := T}=State) ->
   State#{t => tempus:timer(T, timeout)}.

%%
%% spawn new session
connect(Peer, #{strategy := aae, node := Node, adapter := {Mod, Adapter0}, opts := Opts}=State) ->
   ?NOTICE("aae : connect ~p => ~p", [Node, Peer]),
   Adapter1  = Mod:session(Peer, Adapter0),
   Opts1     = [{adapter, {Mod, Adapter1}} | Opts],
   {ok, Pid} = supervisor:start_child(aae_session_sup, [Opts1]),
   erlang:monitor(process, Pid),
   pipe:send(Pid, {session, Node, Peer}),
   State#{adapter => {Mod, Adapter1}}.

%%
%% accept session request
accept({session, Peer} = Req, Pipe, #{node := Node, adapter := {Mod, Adapter0}, opts := Opts}=State) ->
   ?NOTICE("aae : accept ~p => ~p", [Node, Peer]),
   Adapter1  = Mod:session(Peer, Adapter0),
   Opts1     = [{adapter, {Mod, Adapter1}} | Opts],
   {ok, Pid} = supervisor:start_child(aae_session_sup, [Opts1]),
   erlang:monitor(process, Pid),
   pipe:emit(Pipe, Pid, Req),
   State#{adapter => {Mod, Adapter1}}.

%%
%% check capacity
is_allowed(_) ->
   length(aae:i(active)) < opts:val(capacity, ?CONFIG_CAPACITY, aae).


