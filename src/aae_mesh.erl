%%
%% @doc
%%   rumor mongering - mesh topology
-module(aae_mesh).

-export([
   nodes/1,
   peers/2,
   processes/2,
   descend/2,
   equal/2
]).

-type state()  :: pid().
-type key()    :: _.
-type digest() :: _.

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

%%
%% compare values, return true if A =< B
-spec descend(digest(), digest()) -> true | false.

descend(undefined, _) ->
   true;
descend(A, B) ->
   A =< B.

%%
%% compare values, return true if A = B
-spec equal(digest(), digest()) -> true | false.

equal(undefined, _) ->
   false;
equal(A, B) ->
   A =:= B.


