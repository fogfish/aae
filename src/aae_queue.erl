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
%%   active anti-entropy - session queue
-module(aae_queue).
-behaviour(pipe).

-include("aae.hrl").

-export([
   start_link/0
  ,init/1
  ,free/2
  ,ioctl/2
  ,handle/3
   % api
  ,enq/2
]).

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

%%
%%
start_link() ->
   pipe:start_link({local, ?MODULE}, ?MODULE, [], []).

%%
%%
init([]) ->
   lager:md([{lib, aae}]),
   set_t(),
   {ok, handle, q:new()}.

%%
%%
free(_, _) ->
   ok.

%%
%%
ioctl(length, Queue) ->
   q:length(Queue);
ioctl(_, _) ->
   throw(not_implemented).

%%%----------------------------------------------------------------------------   
%%%
%%% api
%%%
%%%----------------------------------------------------------------------------   

%%
%%
enq(Lead, Peer) ->
   pipe:call(?MODULE, {run, Lead, Peer}).

%%%----------------------------------------------------------------------------   
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------   

handle({run, _, _} = Req, Tx, Queue) ->
   pipe:ack(Tx, ok),
   {next_state, handle, session(q:enq(Req, Queue))};

handle(timeout, _, Queue) ->
   set_t(),
   {next_state, handle, session(Queue)}.

%%%----------------------------------------------------------------------------   
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------   

%%
%%
set_t() ->
   tempus:timer(opts:val(session,?CONFIG_SESSION, aae), timeout).

%%
%% schedule aae session
session({}) ->
   {};
session(Queue) ->
   {run, Lead, Peer} = q:head(Queue),
   case aae_leader:session(Lead, Peer) of
      %% no capacity, leader rejected request
      {error, ebusy} ->
         Queue;
      
      ok ->
         q:tail(Queue)
   end.
