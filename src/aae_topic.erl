%% @doc
%%   peer coordination of gossip *topic*
-module(aae_topic).
-behaviour(pipe).

-compile({parse_transform, monad}).

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
   strategy = undefined :: {atom(), _}
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
         strategy = opts:val(strategy, Opts)
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
   ok = cache:put(Cache, Key, Rumor),
   pipe:ack(Pipe, ok),
   {next_state, handle, State};

handle(epidemic, _, State) ->
   infect(State),
   %% @todo: configurable
   erlang:send_after(5000, self(), epidemic),
   {next_state, handle, State};

handle({infect, Snapshot}, _Pipe, #state{strategy = {Strategy, SState}} = State) ->
   lists:foreach(
      fun({Key, Rumor}) ->
         lists:map(
            fun(Pid) ->
               pipe:send(Pid, {gossip, Key, Rumor})
            end,
            Strategy:processes(Key, SState)
         )
      end,
      Snapshot
   ),
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
   stream:fold(
      fun(Rumor, Acc) -> [Rumor | Acc] end, [],
      stream:filter(
         fun({Key, _}) -> lists:member(Peer, Strategy:peers(Key, State)) end,
         hot_rumor(Cache)
      )
   ).

infect_peer(Peer, Snapshot, #state{topic = Topic}) ->
   %% @todo: safe crash here to preserve topic instance 
   pipe:call({aae_peer, Peer}, {infect, Topic, Snapshot}, infinity).

%%
%%
hot_rumor(Cache) ->
   stream( source(Cache) ). 

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
maybeT([]) -> undefined;
maybeT(X)  -> X.






