%%
%% @doc
%%   supervisor of gossip *topic* and attached resources 
-module(aae_topic_sup).
-behaviour(supervisor).

-export([
   start_link/2, init/1
]).

%%
-define(CHILD(Type, I),            {I,  {I, start_link,   []}, permanent, 5000, Type, dynamic}).
-define(CHILD(Type, I, Args),      {I,  {I, start_link, Args}, permanent, 5000, Type, dynamic}).
-define(CHILD(Type, ID, I, Args),  {ID, {I, start_link, Args}, permanent, 5000, Type, dynamic}).

%%
%%
start_link(Name, Opts) ->
   {ok, Sup} = supervisor:start_link(?MODULE, [Name, Opts]),
   Childs    = supervisor:which_children(Sup),
   {_, Cache, _, _} = lists:keyfind(cache, 1, Childs),
   {_, Topic, _, _} = lists:keyfind(aae_topic, 1, Childs),
   ok = pipe:ioctl(Topic, {cache, Cache}),
   {ok, Sup}.

init([Name, Opts]) -> 
   {ok,
      {
         {one_for_all, 4, 1800},
         [
            ?CHILD(worker, cache,     [Opts]),
            ?CHILD(worker, aae_topic, [Name, Opts])
         ]
      }
   }.
