%%
%% @doc
%%   rumor mongering - mesh topology
-module(aae_mesh).

-export([
   nodes/1,
   peers/2,
   processes/2
]).

-type state() :: pid().
-type key()   :: _.

%%
%% return list of susceptible cluster nodes
-spec nodes(state()) -> [node()].

nodes(_State) ->
   erlang:nodes().

%%
%% return list of susceptible peers for given key
-spec peers(key(), state()) -> [node()].

peers(_Key, _State) ->
   erlang:nodes().
 
%%
%% return list of local susceptible processes for given key
-spec processes(key(), state()) -> [pid()].

processes(_Key, State) ->
   [State].
