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
-module(aae_session).
-behaviour(pipe).

-include("aae.hrl").

-export([
   start_link/1
  ,init/1
  ,free/2
  ,ioctl/2
  ,idle/3
  ,handshake/3
  ,snapshot/3
  ,exchange/3
]).

%%
%% internal state specification
% -type(fsm() :: #{
%    adapter=> {atom(), any()}   %% functor
%   ,is     => leader | follower %%
%   ,peer   => pid()
%   ,node   => node()
%   ,t      => any()             %% request timeout      
%   ,ht     => datum:tree()      %% hash / Merkle tree
%   ,stream => datum:stream()    %% storage snapshot
% }).

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

start_link(Opts) ->
   pipe:start_link(?MODULE, [Opts], []).

init([Opts]) ->
   lager:md([{lib, aae}]),
   {ok, idle, lists:foldl(fun init/2, #{}, Opts)}.

init({timeout, X}, State) ->
   State#{t => X};
init({adapter, X}, State) ->
   State#{adapter => X};
init(_, State) ->
   State.

free(Reason, #{adapter := {Mod, Adapter}}) ->
   Mod:free(Reason, Adapter),
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

idle({session, Node, Peer}, _Pipe, #{adapter := {Mod, Adapter0}}=State) ->
   Adapter1  = Mod:session(Peer, Adapter0),
   {next_state, handshake, 
      set_timeout(req_handshake(Node, Peer, State#{adapter => {Mod, Adapter1}}))
   };

%%
idle({session, Peer}, Pipe, #{adapter := {Mod, Adapter0}}=State) ->
   Adapter1  = Mod:session(Peer, Adapter0),
   erlang:monitor(process, pipe:a(Pipe)),
   pipe:a(Pipe, {session, ack}),
   ?INFO("aae : -handshake ~p (~s)", [Peer, erlang:node(pipe:a(Pipe))]),
   snapshot(prepare, Pipe, 
      State#{
         is      => follower 
        ,peer    => Peer
        ,adapter => {Mod, Adapter1}
      }
   ).

%%%
%%% HANDSHAKE
%%%

%%
handshake(timeout, _Pipe, State) ->
   {stop, timeout, State};

%%
handshake({session, nok}, _Pipe, State) ->
   {stop, normal,  State};

%%
handshake({session, ack}, Pipe, #{peer := Peer, t := T}=State) ->
   erlang:monitor(process, pipe:a(Pipe)),
   ?INFO("aae : +handshake ~p (~s)", [Peer, erlang:node(pipe:a(Pipe))]),
   snapshot(prepare, Pipe, 
      State#{
         is => leader
        ,t  => tempus:cancel(T)
      }
   ).

%%%
%%% SNAPSHOT
%%%

%%
snapshot(prepare, Pipe, #{adapter := {Mod, Adapter0}} = State) ->
   %% @todo: log snapshot time
   pipe:emit(Pipe, self(), build),
   {Stream, Adapter1} = Mod:snapshot(Adapter0),
   {next_state, snapshot, 
      State#{
         stream  => Stream
        ,ht      => htree:new()
        ,adapter => {Mod, Adapter1}
      }
   };
      
snapshot(build, Pipe, #{ht := HT0, stream := Stream0}=State) ->
   case htbuild(HT0, Stream0) of
      %% hash tree is build
      {eof,     HT1} ->
         exchange(init, Pipe, 
            State#{
               ht     => HT1, 
               stream => {}
            }
         );

      %% hash tree is not build 
      {HT1, Stream1} ->
         snapshot(build, Pipe, State#{ht => HT1, stream => Stream1})
         %% 
         %% @todo: disable async builder due to sync issue with leader
         %%
         %% %% @todo: throttle build procedure
         %% pipe:emit(Pipe, self(), build),
         %% {next_state, snapshot,
         %%   State#{
         %%      ht     => HT1,
         %%      stream => Stream1
         %%   }
         %%}
   end;

%% abort gossip session due to peer failure
snapshot({'DOWN', _, _, _Peer, _Reason}, _Pipe, State) ->
   ?WARNING("aae : peer failed ~p (~s)", [_Reason, erlang:node(_Peer)]),
   {stop, normal, State}.


%%%
%%% EXCHANGE
%%%

%% kick-off reconciliation 
exchange(init, _Pipe, #{is := follower}=State) ->
   {next_state, exchange, State#{lsync => 0}};

exchange(init, Pipe, #{is := leader, ht := HT}=State) ->
   %% @todo: leader process shall wait for follower.
   %%        it must start reconciliation only when follower built its hash tree
   case htree:hash(0, HT) of
      undefined ->
         HashB = htree:hash(HT),
         pipe:a(Pipe, HashB),
         ?DEBUG("gossip [ae]: ~p send ~p", [self(), dump(HashB)]),
         {next_state, exchange, State#{lsync => 1}};

      HashB     ->
         pipe:a(Pipe, HashB),
         ?DEBUG("gossip [ae]: ~p send ~p", [self(), dump(HashB)]),
         {next_state, exchange, State#{lsync => 1}}
   end;

%% reconciliation is over, list of leaf hashes is proposed
exchange({hash, -1, _}=HashA, Pipe, #{peer := Peer, ht := HT0, adapter := {Mod, Adapter0}}=State) ->
   ?DEBUG("gossip [ae]: ~p recv ~p", [self(), dump(HashA)]),
   HashB = htree:hash(HT0),
   pipe:a(Pipe, HashB),   
   ?DEBUG("gossip [ae]: ~p send ~p", [self(), dump(HashB)]),
   Diff  = htree:diff(HashA, HashB),
   ?DEBUG("gossip [ae]: diff ~p (~s) ~p", [pipe:a(Pipe), erlang:node(pipe:a(Pipe)), dump(Diff)]),
   htree:foreach(
      fun(_Hash, Val) ->
         Mod:diff(Peer, Val, Adapter0)
      end,
      htree:evict(Diff, HT0)
   ),
   {stop, normal, State};

%% intermediate hashes is proposed
exchange({hash,  _, _}=HashA, Pipe, #{lsync := L, ht := HT0}=State) ->
   ?DEBUG("gossip [ae]: ~p recv ~p", [self(), dump(HashA)]),
   case htree:hash(L, HT0) of
      undefined ->
         HashB = htree:hash(HT0),
         pipe:a(Pipe, HashB),
         ?DEBUG("gossip [ae]: ~p send ~p", [self(), dump(HashB)]),

         {next_state, exchange, State};

      HashB     ->
         pipe:a(Pipe, HashB),
         ?DEBUG("gossip [ae]: ~p send ~p", [self(), dump(HashB)]),
         HT = htree:evict(htree:diff(HashA, HashB), HT0),
         {next_state, exchange, State#{lsync => L + 1, tree => HT}}          
   end;

%% abort gossip session due to peer failure
exchange({'DOWN', _, _, _Peer, _Reason}, _Pipe, State) ->
   ?WARNING("gossip [ae]: peer failure ~p", [_Reason]),
   {stop, normal, State}.

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
%% initiate handshake
req_handshake(Node, Peer, #{adapter := {Mod, Adapter0}}=State) ->
   Adapter1 = Mod:handshake(Peer, {session, Node}, Adapter0),
   State#{peer => Peer, adapter => {Mod, Adapter1}}.


%%
%%
dump({hash, L, Hash}) ->
   {hash, L, gb_sets:size(Hash)}.

%%
%% build hash tree 
htbuild(Ht, Stream) ->
   htbuild(?CONFIG_IO_CHUNK, Ht, Stream).

htbuild(_, HT,     {}) ->
   {eof, HT};

htbuild(0, HT, Stream) ->
   {HT, Stream};

htbuild(N, HT, Stream) ->
   Key = stream:head(Stream),
   htbuild(N - 1, htree:insert(Key, Key, HT), stream:tail(Stream)).

