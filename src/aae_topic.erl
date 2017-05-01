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
   n        = undefined :: integer()
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
   %% @todo: configurable
   erlang:send_after(5000, self(), epidemic),
   {ok, handle, 
      #state{
         topic    = Uid,
         strategy = opts:val(strategy, Opts),
         n        = opts:val(n, Opts)
      }
   }.

free(_Reason, _State) ->
   ok.

ioctl({cache, Pid}, State) ->
   State#state{cache = Pid}.

%%%----------------------------------------------------------------------------
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------

handle({gossip, Key, Rumor}, Pipe, #state{cache = Cache} = State) ->
   ok = cache:put(Cache, Key, {hot, Rumor}),
   pipe:ack(Pipe, ok),
   {next_state, handle, State};

handle(epidemic, _, State) ->
   infect(State),
   %% @todo: configurable
   erlang:send_after(1000, self(), epidemic),
   {next_state, handle, State};

handle(#pushpull{peer = Peer, digest = Digest}, _Pipe, #state{topic = Topic, strategy = {Strategy, SState}} = State) ->
   {Susceptible, Infective, Removed} = splitwith(Digest, State),
   io:format("[aae] : pushpull ~p / ~p S: ~p I: ~p R: ~p~n", [Peer, length(Digest), length(Susceptible), length(Infective), length(Removed)]),

   forget(Removed, State),
   infect_local_processes(Susceptible, State),

   %% feedback 
   Feedback = [{Key, A} || {Key, A, _} <- Infective ++ Removed],
   pipe:send({aae_peer, Peer}, #feedback{peer = node(), topic = Topic, digest = Feedback}),

   {next_state, handle, State};

handle(#feedback{peer = Peer, digest = Digest}, _Pipe, #state{cache = Cache} = State) ->
   {Susceptible, _Infective, Removed} = splitwith(Digest, State),
   io:format("[aae] : feedback ~p / ~p S: ~p I: ~p R: ~p~n", [Peer, length(Digest), length(Susceptible), length(_Infective), length(Removed)]),

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

infect(State) ->
   do([m_maybe ||
      Peer <- find_susceptible_peer(State),
      Snap <- maybeT( build_snapshot(Peer, State) ),
      _    <- infect_peer(Peer, Snap, State),
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
build_snapshot(Peer, #state{cache = Cache, strategy = {Strategy, State}}) ->
   %% @todo: stream random
   %% @todo: stream limit to MTU
   % stream:fold(
      % fun({Key, {_, A}}, Acc) -> [{Key, A} | Acc] end, [],
      % stream:take(128,
         uniform(
            stream:filter(
               fun({Key, _}) -> lists:member(Peer, Strategy:peers(Key, State)) end,
               hot_rumor(Cache)
            )
         ).
      % )
   % ). 

infect_peer(Peer, Snapshot, #state{topic = Topic}) ->
   %% @todo: safe crash here to preserve topic instance
   % io:format("======>~n~p~n", [Snapshot]), 
   pipe:call({aae_peer, Peer}, #pushpull{peer = node(), topic = Topic, digest = Snapshot}, infinity).


%%
%%
hot_rumor(Cache) ->
   stream:filter(
      fun({_, {Val, _}}) -> Val =:= hot end,
      stream( source(Cache) )
   ).

source(Cache) ->
   %% @todo: probability based approach to spread rumor
   hd( cache:i(Cache, heap) ).

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
uniform(Stream) ->
   uniform(1, 1024, Stream).

% uniform(_, {}) ->
   % stream:new();   
uniform(N, K, Stream) ->
   q:list(
      erlang:element(2, stream:fold(
         fun({Key, {_, A}}, {N0, Queue}) ->
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
      ))
   ).



   % X 

   % case rand:uniform(N) of 
   %    1 -> 
   %       stream:new(stream:head(Stream), 
   %          fun() -> uniform(N + 1, stream:tail(Stream)) end);
   %    _ ->
   %       uniform(N + 1, stream:tail(Stream))
   % end.

%%
%% split digest on susceptible and removed rumor
splitwith(Gossip, #state{cache = Cache, strategy = {Strategy, _}}) ->
   Digest = lists:map(
      fun({Key, B}) ->
         case cache:lookup(Cache, Key) of
            undefined ->
               {Key, undefined, B};
            {_, A} ->
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
forget(Digest, #state{cache = Cache, n = N}) ->
   lists:filter(
      fun({Key, A, _}) ->
         case rand:uniform(N) of
            1 ->
               % cache:put(Cache, Key, {dead, A}),
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






