%% @doc
%%   peer coordination of gossip *topic*
-module(aae_topic).
-behaviour(pipe).

-compile({parse_transform, monad}).
-include("aae.hrl").

-export([
   start_link/2
  ,init/1
  ,free/2
  ,ioctl/2
  ,handle/3
]).

%% internal state
-record(state, {
   topic    = undefined :: _,
   cache    = undefined :: pid(),
   strategy = undefined :: {atom(), _},
   n        = undefined :: integer(),
   cycle    = undefined :: tempus:timer(),
   packet   = undefined :: integer(),
   evidence = undefined :: _ 
}).

%%%----------------------------------------------------------------------------   
%%%
%%% factory
%%%
%%%----------------------------------------------------------------------------   

start_link(Uid, Opts) ->
   pipe:start_link(?MODULE, [Uid, Opts], []).

init([Uid, Opts]) ->
   pns:register(aae, Uid, self()),
   {ok, handle, 
      #state{
         topic    = Uid,
         strategy = opts:val(strategy, Opts),
         n        = opts:val(n, Opts),
         cycle    = tempus:timer(opts:val(cycle, ?CONFIG_CYCLE, Opts), epidemic),
         packet   = opts:val(packet, ?CONFIG_PACKET, Opts),
         evidence = orddict:new()
      }
   }.

free(_Reason, _State) ->
   ok.


ioctl(i, #state{evidence = Ev}) ->
   Ev;
ioctl({cache, Pid}, State) ->
   State#state{cache = Pid}.

%%%----------------------------------------------------------------------------
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------

handle({gossip, Key, Digest}, Pipe, #state{cache = Cache} = State) ->
   pipe:ack(Pipe, cache:put(Cache, Key, Digest)),
   {next_state, handle, State};

handle(epidemic, _, #state{cycle = Cycle} = State) ->
   infect_neighborhood(State),
   {next_state, handle, State#state{cycle = tempus:timer(Cycle, epidemic)}};

handle(#pushpull{peer = Peer, digest = Digest}, _Pipe, #state{evidence = Tree} = State) ->
   {Susceptible, Infective, Removed} = splitwith(Digest, State),
   % io:format("[aae] : pushpull ~p / ~p S: ~p I: ~p R: ~p~n", [Peer, length(Digest), length(Susceptible), length(Infective), length(Removed)]),
   forget(Removed, State),
   infect_local_processes(Susceptible, State),
   peer_feedback(Peer, [{Key, A} || {Key, A, _} <- Infective ++ Removed], State),
   Evidence = #{
      susceptible => length(Susceptible),
      infective => length(Infective),
      removed => length(Removed)
   },
   {next_state, handle, State#state{evidence = orddict:store(Peer, Evidence, Tree)}};

handle(#feedback{peer = _Peer, digest = Digest}, _Pipe, #state{} = State) ->
   {Susceptible, _Infective, Removed} = splitwith(Digest, State),
   % io:format("[aae] : feedback ~p / ~p S: ~p I: ~p R: ~p~n", [Peer, length(Digest), length(Susceptible), length(_Infective), length(Removed)]),
   forget(Removed, State),
   infect_local_processes(Susceptible, State),
   {next_state, handle, State};

handle(_, _, State) ->
   {next_state, handle, State}.


%%%----------------------------------------------------------------------------
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------

%%
%%
infect_neighborhood(State) ->
   do([m_maybe ||
      Peer <- find_susceptible_peer(State),
      Snap <- maybeT( build_digest_snapshot(Peer, State) ),
      _    <- peer_pushpull(Peer, Snap, State),
      return(_)
   ]).

%%
%%
find_susceptible_peer(#state{strategy = {Strategy, State}}) ->
   do([m_maybe ||
      List <- maybeT( Strategy:nodes(State) ),
      _    <- lists:nth(rand:uniform(length(List)), List),
      return(_) 
   ]).

%%
%%
build_digest_snapshot(Peer, #state{cache = Cache, packet = Packet, strategy = {Strategy, State}}) ->
   packetize(Packet,
      stream:filter(
         fun({Key, _}) -> lists:member(Peer, Strategy:peers(Key, State)) end,
         hot_rumor(Cache)
      )
   ).

peer_pushpull(Peer, Snapshot, #state{topic = Topic}) ->
   %% @todo: safe crash here to preserve topic instance
   pipe:call({aae_peer, Peer}, #pushpull{peer = node(), topic = Topic, digest = Snapshot}, infinity).

peer_feedback(Peer, Snapshot, #state{topic = Topic}) ->
   pipe:send({aae_peer, Peer}, #feedback{peer = node(), topic = Topic, digest = Snapshot}).



%%
%%
hot_rumor(Cache) ->
   stream( source_heap(Cache) ).

%%
%% use probabilistic approach to choose cache segment
%% 1/1, 1/2, 1/3, 1/4, ..., 1/n
source_heap(Cache) ->
   erlang:element(2,
      lists:foldl(
         fun match_heap/2, 
         {1, undefined}, 
         cache:i(Cache, heap)
      )
   ).

match_heap(Heap, {N, Some}) ->
   case rand:uniform(N) of
      1 -> {N + 1, Heap};
      _ -> {N + 1, Some}
   end.

%%
%% ets stream
stream(FD) ->
   case ets:lookup(FD, ets:first(FD)) of
      [{Key, _} = Head] ->
         stream:new(Head, fun() -> stream(FD, Key) end);   
      [] ->
         stream:new()
   end.

stream(FD, Key0) ->
   case ets:next(FD, Key0) of
      '$end_of_table' ->
         stream:new();
      Key1 ->
         [{Key, _} = Head] = ets:lookup(FD, Key1),
         stream:new(Head, fun() -> stream(FD, Key) end)
   end.

%%
%%
packetize(Packet, Stream) ->
   uniform(1, Packet, Stream).

uniform(N, K, Stream) ->
   q:list(
      erlang:element(2, 
         stream:fold(
            fun({Key, A}, {N0, Queue}) ->
               case rand:uniform(N0) of
                  X when X =< K ->
                     Q = q:enq({Key, A}, Queue),
                     case q:length(Q) > K of
                        true  ->
                           {_, Q1} = q:deq(Q),
                           {N0 + 1, Q1};
                        false ->
                           {N0 + 1, Q}
                     end;
                  _ ->
                     {N0 + 1, Queue}
               end
            end,
            {N, q:new()},
            Stream
         )
      )
   ).

%%
%% split digest on susceptible and removed rumor
splitwith(Gossip, #state{cache = Cache, strategy = {Strategy, _}}) ->
   Digest = lists:map(
      fun({Key, B}) ->
         case cache:lookup(Cache, Key) of
            undefined ->
               {Key, undefined, B};
            A ->
               {Key, A, B}
         end
      end,
      Gossip
   ),
   {Removed, List} = lists:partition(
      fun({_, A, B}) -> Strategy:equal(A, B) end, 
      Digest
   ),
   {Susceptible, Infective} = lists:partition(
      fun({_, A, B}) -> Strategy:descend(A, B) end,
      List
   ),
   {Susceptible, Infective, Removed}.

%%
%% forget rumor with 1/n probability 
forget(Digest, #state{cache = _Cache, n = N}) ->
   lists:filter(
      fun({_Key, _A, _}) ->
         case rand:uniform(N) of
            1 ->
               % cache:touch(Cache, Key, TTL div N),
               true;
            _ ->
               false
         end
      end,
      Digest
   ).


%%
%%
infect_local_processes(Digest, #state{strategy = {Strategy, State}}) ->
   lists:foreach(
      fun({Key, _, B}) ->
         lists:map(
            fun(Pid) ->
               %% Note: async notification
               pipe:send(Pid, {gossip, Key, B})
            end,
            Strategy:processes(Key, State)
         )
      end,
      Digest
   ).

%%
%%
maybeT([]) -> undefined;
maybeT(X)  -> X.

