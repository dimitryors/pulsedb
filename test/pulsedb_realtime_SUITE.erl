-module(pulsedb_realtime_SUITE).
-compile(export_all).
-define(TICK_COUNT, 3).


all() ->
  [{group, subscribe}].


groups() ->
  [{subscribe, [parallel], [
    subscribe,
    subscribe_and_die,
    % subscribe_twice,
    unsubscribe
  ]}].


init_per_suite(Config) ->
  application:start(pulsedb),
  Config.


end_per_suite(Config) ->
  application:stop(pulsedb),
  Config.

subscribe(_) ->
  N = ?TICK_COUNT,
  {UTC,Delay} = pulsedb:current_second(),
  pulsedb_realtime:subscribe(<<"input">>, i),
  Self = self(),
  spawn_link(fun () -> send_tick(100, {<<"input">>, UTC-100, 1, [{<<"name">>,<<"src1">>}]}), Self ! go end),
  receive go -> ok after 1000 -> error(timeout) end,
  ok = collect_ticks(N,i,Delay).


subscribe_and_die(_) ->
  N = ?TICK_COUNT,
  {UTC,_Delay} = pulsedb:current_second(),
  Self = self(),
  C1 = spawn(fun () ->
               pulsedb_realtime:subscribe(<<"input4">>, i),
               Self ! c1_subscribed,
               receive die -> ok
               after 1000 -> error(c1_not_dead)
               end
             end),
  
  C2 = spawn(fun () ->
               pulsedb_realtime:subscribe(<<"input4">>, i2),
               Self ! c2_subscribed,
               receive go -> ok
               after 1000 -> error(timeout)
               end,
               ok = collect_ticks(N,i2,1200),
               Self ! c2_ok
             end),
  
  erlang:monitor(process, C1),
  
  receive c1_subscribed -> ok
  after 10 -> error(c1_not_subscribed)
  end,
  
  receive c2_subscribed -> ok
  after 10 -> error(c2_not_subscribed)
  end,
  
  C1 ! die,
  receive {'DOWN', _, _, C1, _} -> ok
  after 10 -> error(c1_not_dead)
  end,
  
  spawn_link(fun () -> 
               send_tick(100, {<<"input4">>, UTC-100, 1, [{<<"name">>,<<"src1">>}]}), 
               C2 ! go,
               Self ! go
             end),
  
  receive go -> ok
  after 1000 -> error(timeout)
  end,
  
  receive c2_ok -> ok
  after 2000 -> error(c2_not_received_pulse)
  end.
%   N = ?TICK_COUNT,
%   {UTC,Delay} = pulsedb:current_second(),
%   ct:pal("DELAY ~p", [Delay]),
%   pulsedb_realtime:subscribe(<<"input4">>, i),
%   pulsedb_realtime:subscribe(<<"input4">>, i2),
%   Self = self(),
%   spawn_link(fun () -> send_tick(100, {<<"input">>, UTC-100, 1, [{<<"name">>,<<"src1">>}]}), Self ! go end),
%   receive go -> ok after 1000 -> error(timeout) end,
  
%   collect_ticks(N,1200),
  
%   receive {pulse, i2, _,_} -> ok
%   after 10 -> ct:pal("messages: ~p", [process_info(self(),messages)]), error(i2_not_collected)
%   end.


% subscribe_twice(_) ->
%   N = ?TICK_COUNT,
%   {UTC,_} = pulsedb:current_second(),
%   pulsedb_realtime:subscribe(<<"input">>, i),
%   pulsedb_realtime:subscribe(<<"input">>, i),
%   Self = self(),
%   spawn_link(fun () -> send_tick(100, {<<"input">>, UTC-100, 1, [{<<"name">>,<<"src1">>}]}), Self ! go end),
%   receive go -> ok after 1000 -> error(timeout) end,
%   ok = collect_ticks(N,500),
%   receive
%     _ -> error(received_pulse)
%   after 2000 -> ok 
%   end.

unsubscribe(_) ->
  pulsedb_realtime:subscribe(<<"input2">>, i),
  [{i,<<"input2">>}] = pulsedb_realtime:subscriptions_for_pid(self()),
  pulsedb_realtime:subscribe(<<"input5">>, j),
  [{i,<<"input2">>},{j,<<"input5">>}] = pulsedb_realtime:subscriptions_for_pid(self()),

  pulsedb_realtime:unsubscribe(i),
  [{j,<<"input5">>}] = pulsedb_realtime:subscriptions_for_pid(self()),
  ok.
  
  
send_tick(0, _) -> ok;
send_tick(N, {Name, UTC, Value, Tags}=Pulse) ->
  pulsedb_memory:append(Pulse, seconds),
  Pulse1 = {Name, UTC+1, Value, Tags},
  timer:sleep(2),
  send_tick(N-1, Pulse1).
  

collect_ticks(0,_,_) -> ok;
collect_ticks(N,Tag,Timeout) ->
  receive
    {pulse, Tag, _,_} -> collect_ticks(N-1,Tag,Timeout)
  after
    Timeout -> ct:pal("Message queue: ~p when still need to read ~p ticks with tag ~p", [
      process_info(self(),messages), N, Tag
    ]), error(collect_timed_out)
  end.
