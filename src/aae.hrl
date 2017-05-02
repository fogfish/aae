
%%
%%
-record(pushpull, {
   peer   = undefined :: node(),
   topic  = undefined :: binary(),
   digest = undefined :: [{_, _}]
}).

%%
%%
-record(feedback, {
   peer   = undefined :: node(),
   topic  = undefined :: binary(),
   digest = undefined :: [{_, _}]
}).


%%
%% default gossip cycle
-define(CONFIG_CYCLE,     1000).

%%
%% default gossip packet
-define(CONFIG_PACKET,    1024).




%%
%% default timeout to active anti-entropy session
-define(CONFIG_SESSION,  60000).

%%
%% default aae capacity
-define(CONFIG_CAPACITY,     5).

%%
%% number of items to read w/o interruption
%% it defines amount of waisted i/o if peer dies
-define(CONFIG_IO_CHUNK,     10000).


%%%----------------------------------------------------------------------------   
%%%
%%% logger macro 
%%%
%%%----------------------------------------------------------------------------   

%% 
%% logger macros
%%   debug, info, notice, warning, error, critical, alert, emergency
-ifndef(EMERGENCY).
-define(EMERGENCY(Fmt, Args), lager:emergency(Fmt, Args)).
-endif.

-ifndef(ALERT).
-define(ALERT(Fmt, Args), lager:alert(Fmt, Args)).
-endif.

-ifndef(CRITICAL).
-define(CRITICAL(Fmt, Args), lager:critical(Fmt, Args)).
-endif.

-ifndef(ERROR).
-define(ERROR(Fmt, Args), lager:error(Fmt, Args)).
-endif.

-ifndef(WARNING).
-define(WARNING(Fmt, Args), lager:warning(Fmt, Args)).
-endif.

-ifndef(NOTICE).
-define(NOTICE(Fmt, Args), lager:notice(Fmt, Args)).
-endif.

%%
-ifndef(INFO).
-define(INFO(Fmt, Args), lager:info(Fmt, Args)).
-endif.

%% 
-ifdef(CONFIG_DEBUG).
   -define(DEBUG(Str, Args), lager:debug(Str, Args)).
-else.
   -define(DEBUG(Str, Args), ok).
-endif.
