-module(pulsedb_realtime).
-export([subscribe/2, unsubscribe/1]).
-export([subscriptions_for_pid/1]).
-export([start_link/0, init/1, terminate/2, handle_info/2, handle_call/3]).

-record(subscriptions, 
 {
  clients=[] % query_key, {Name,Query}, pids
  }).


subscribe(Query0, Tag) ->
  case pulsedb_query:parse(Query0) of
    {error, _} = Error ->
      Error;
    Query1 ->
      Query = pulsedb_query:remove_tag([from,to],Query1),
      gen_server:call(?MODULE, {subscribe, self(), Query, Tag})
  end.



unsubscribe(Tag) ->
  gen_server:call(?MODULE, {unsubscribe, self(), Tag}).


subscriptions_for_pid(Pid) ->
  gen_server:call(?MODULE, {subscriptions_for,Pid}).

start_link() -> 
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


init([]) ->
  erlang:send_after(0, self(), tick),
  {ok, #subscriptions{}}.


terminate(_,_) ->
  ok.


handle_call({subscribe, Pid, Query, Tag},_, #subscriptions{clients=Clients0}=State) ->
  Clients = case lists:keytake(Query, 1, Clients0) of
    {value, {Q, UTC, Pids0}, Rest} ->
      Pids = case find_subscription(Pid, Tag, Pids0) of
        {[], Pids0} -> 
          Ref = erlang:monitor(process, Pid),
          [{Pid, Tag, Ref}|Pids0];
        _ -> 
          Pids0
      end,
      [{Q,UTC,Pids} | Rest];
    false ->
      Ref = erlang:monitor(process, Pid),
      [{Query,undefined,[{Pid,Tag,Ref}]} | Clients0]
  end,
  {reply, ok, State#subscriptions{clients=Clients}};


handle_call({subscriptions_for, Pid}, _, #subscriptions{clients = Clients} = State) ->
  Subscriptions = lists:flatmap(fun({Query,_,Pids}) ->
    [{Tag,pulsedb_query:render(Query)} || {P,Tag,_} <- Pids, P == Pid]    
  end, Clients),
  {reply, lists:sort(Subscriptions), State};


handle_call({unsubscribe, Pid, Tag},_, #subscriptions{clients=Clients0}=State) ->
  Clients1 = 
  [begin
    {Demonitor, Rest} = find_subscription(Pid, Tag, Pids),
    [erlang:demonitor(Ref) || {_,_,Ref} <- Demonitor],
    {Query, UTC, Rest}
  end || {Query,UTC,Pids} <- Clients0],
  Clients = [C || {_,_,Pids}=C <- Clients1, length(Pids) > 0],
  {reply, ok, State#subscriptions{clients=Clients}}.




handle_info(tick, #subscriptions{clients=Clients}=State) ->
  {Now,_} = pulsedb:current_second(),
  T1 = os:timestamp(),
  Clients1 = lists:map(fun({Query,LastUTC,Pids} = Entry) ->
    Step = pulsedb_query:downsampler_step(Query),
    To = Now - 4,
    From = To - Step * 3,
    Q1 = pulsedb_query:set_range(From, To, Query),
    Q2 = pulsedb_query:render(Q1),
    case pulsedb:read(Q2, memory) of
      {ok, [], _} -> 
        Entry;
      {ok, Data, _} ->
        Data1 = [{UTC,V} || {UTC,V} <- Data, UTC > LastUTC orelse LastUTC == undefined],
        if
          Data1 == [] ->
            Entry;
          true ->
            {Last,_} = lists:last(Data1),
            [Pid ! {pulse, Tag, UTC, V} || {Pid,Tag,_} <- Pids, {UTC,V} <- Data1],
            {Query, Last, Pids}
        end                 
    end
  end, Clients),
  T2 = os:timestamp(),
  case timer:now_diff(T2,T1) of
    T when T > 1000 -> lager:debug("tick took ~B us", [timer:now_diff(T2,T1)]);
    _ -> ok
  end,
  {_,Delay} = pulsedb:current_second(),
  erlang:send_after(Delay, self(), tick),
  {noreply, State#subscriptions{clients = Clients1}};



handle_info({'DOWN', _, _, Pid, _}, #subscriptions{clients=Clients0}=State) ->
  Clients1 = [{Query, UTC, lists:keydelete(Pid,1,Pids)} 
             || {Query, UTC, Pids} <- Clients0],
  Clients = [C || {_,_,Pids}=C <- Clients1, length(Pids) > 0],
  {noreply, State#subscriptions{clients=Clients}}.


find_subscription(Pid, Tag, Pids) ->
  lists:partition(fun ({Pid0,Tag0,_}) ->
                    Pid0 == Pid andalso Tag0 == Tag
                  end, Pids).
