%% 
%% @doc
%%
-module(aae_peer).
-behaviour(pipe).

-compile({parse_transform, category}).

-export([
   start_link/0
  ,init/1
  ,free/2
  ,handle/3
]).

%% internal state
-record(state, {
}).

%%%----------------------------------------------------------------------------   
%%%
%%% factory
%%%
%%%----------------------------------------------------------------------------   

start_link() ->
   pipe:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
   {ok, handle, #state{}}.

free(_Reason, _State) ->
   ok.


%%%----------------------------------------------------------------------------
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------

handle({infect, Topic, Snapshot}, Pipe, State) ->
   case pns:whereis(aae, Topic) of
      undefined ->
         pipe:ack(Pipe, {error, noroute});
      Pid ->
         %% @todo: think about flow control
         pipe:send(Pid, {infect, Snapshot}),
         pipe:ack(Pipe, ok)
   end,
   {next_state, handle, State};

handle(_, _, State) ->
   {next_state, handle, State}.
