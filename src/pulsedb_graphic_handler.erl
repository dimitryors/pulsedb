-module(pulsedb_graphic_handler).
-behaviour(cowboy_websocket_handler).
-behaviour(cowboy_http_handler).

-export([init/3, handle/2, terminate/3]).
-export([websocket_init/3]).
-export([websocket_handle/3]).
-export([websocket_info/3]).
-export([websocket_terminate/3]).

-export([pulse_data/3, make_queries/1]).

-define(HTTP_REQUEST_TIMEOUT, 5000).
-define(WS_TIMEOUT, 3000).

-record(ws_state, {
  last_utc = [],
  pulses,
  db,
  timer,
  ip
}).

-record(page_state, {
  embed
}).


init({tcp, http}, Req, _Opts) -> 
  {Path, Req2} = cowboy_req:path_info(Req),
  case Path of
    [_,<<"events">>] -> {upgrade, protocol, cowboy_websocket};
    [Embed]          -> {ok, Req2, #page_state{embed = Embed}};
    _                -> {shutdown, Req2, undefined}
  end.

terminate(_,_,_) ->
  ok.

handle(Req, #page_state{embed=Embed}=State) ->
  {ok, Reply} = 
  case resolve_embed(Embed) of
    {ok, Title, _} ->
      {Path, Req5} = cowboy_req:path(Req),
      InitData = [{title, Title},
                  {embed, Embed},
                  {ws_path, filename:join([Path,"events"])}],
      Page = fill_template(ok, InitData),
      cowboy_req:reply(200, headers(html), Page, Req5);
    {deny, Message} ->
      cowboy_req:reply(403, headers(html), fill_template(denied, [{message, Message}]), Req);
    {not_found, Message} -> 
      cowboy_req:reply(404, headers(html), fill_template(not_found, [{message, Message}]), Req);
    {error, Message} -> 
      cowboy_req:reply(500, headers(html), fill_template(error, [{message, Message}]), Req)
  end,
  {ok, Reply, State}.


websocket_init(_Transport, Req, Opts) ->
  {Ip, Req1} = case cowboy_req:header(<<"x-real-ip">>, Req) of
    {undefined, Req_} ->
      {{PeerAddr,_}, Req1_} = cowboy_req:peer(Req_),
      {list_to_binary(inet_parse:ntoa(PeerAddr)), Req1_};
    {PeerAddr, Req1_} ->
      {PeerAddr, Req1_}
  end,
  put(name, {pulsedb_graph,Ip}),
  self() ! init,
  DB = proplists:get_value(db,Opts),
  {ok, Req1, #ws_state{ip = Ip, db = DB}, 2*?WS_TIMEOUT}.


websocket_handle({pong, _}, Req, #ws_state{} = State) ->
  Ref = erlang:send_after(?WS_TIMEOUT, self(), ping),
  {ok, Req, State#ws_state{timer = Ref}};  


websocket_handle({text, Text}, Req, #ws_state{}=State) ->
  Json = jsx:decode(Text),
  Embed = proplists:get_value(<<"embed">>, Json),
  {ok, Title, Queries} = resolve_embed(Embed),
  case pulse_data(Title, Queries, State) of
    {ok, InitBody, NewState} ->
      {reply, {text, jsx:encode(InitBody)}, Req, NewState};
    _ ->
      {shutdown, Req, State}
  end;

websocket_handle(Data, Req, State) -> 
  lager:info("Unknown request ~p", [Data]),
  {ok, Req, State}.



pulse_data(Title, Queries, #ws_state{db = DB} = State) ->
  {History, PulseTokens, LastUTCs1} = lists:unzip3(
  [begin
    {Name, QueryRealtime, QueryHistory} = make_queries(Query),
    {ok,History1,_} = pulsedb:read(QueryHistory, DB),
    HistoryData = [[T*1000, V] || {T, V} <- History1],

    Token = make_ref(),
    pulsedb:subscribe(QueryRealtime, Token),
    lager:info("Subscribed websocket ~p to pulse ~s", [get(name), QueryRealtime]),
     
    Link = {Name, Token},
    History2 = [{name,Name},{data, HistoryData}],

    LastUTC = case History1 of
      [] -> undefined;
      _ -> [{Token, element(1,lists:last(History1)) }]
    end,
     
     {History2, Link, LastUTC}
    end
   || Query <- Queries]),
  
  
  LastUTCs = [L || L <- LastUTCs1, is_tuple(L)],
  Config = [
    {title, Title}
  ],
  Reply = [{init, true}, {options, Config}, {data, History}],
  {ok, Reply, State#ws_state{pulses=PulseTokens, last_utc = LastUTCs}}.



websocket_info({pulse, Token, UTC, Value}, Req, #ws_state{pulses=Pulses, last_utc = LastUTCs}=State) ->
  case lists:keyfind(Token, 2, Pulses) of
    false -> 
      {noreply, State};
    {Name,Token} ->
      case lists:keyfind(Token, 1, LastUTCs) of
        {_, LastUTC} when LastUTC - UTC >= 0 ->
          {noreply, State};
        _ ->
          Points = [UTC*1000,Value],
          Prepared = [{shift,true}, {Name, [Points]}],
          {reply, {text, jsx:encode(Prepared)}, Req, State}
      end
  end;

websocket_info(init, Req, #ws_state{} = State) ->
  Ref = erlang:send_after(?WS_TIMEOUT, self(), ping),
  {ok, Req, State#ws_state{timer = Ref}};

websocket_info(ping, Req, #ws_state{timer = Old} = State) ->
  erlang:cancel_timer(Old),
  {reply, {ping, <<>>}, Req, State#ws_state{timer = undefined}};


websocket_info(Msg, Req, #ws_state{}=State) ->
  lager:info("Unknown message ~p", [Msg]),
  {ok, Req, State}.



websocket_terminate(Reason, _Req, #ws_state{ip = Ip}) ->
  lager:info("Closing graphic connection to ip ~s due to ~p", [Ip, Reason]),
  ok.


fill_template(Status, Data) ->
  TemplateName = iolist_to_binary(io_lib:format("templates/embed_~p.html", [Status])),
  Path = filename:join(code:lib_dir(pulsedb,webroot), TemplateName),
  {ok, Template} = file:read_file(Path),
  lists:foldl(fun ({Name_, Value}, T) ->
                Name = iolist_to_binary(io_lib:format("{{~p}}", [Name_])),
                re:replace(T, Name, Value, [{return, binary}])
              end,
              Template, Data).


headers(json) -> [{<<"content-type">>,<<"application/json">>}];
headers(html) -> [{<<"content-type">>, <<"text/html">>}].



make_queries(Query0) ->
  {Now,_} = pulsedb:current_second(),
  {_,_,Name,_} = Query1 = pulsedb_query:parse(Query0),
  
  Query2 = pulsedb_query:remove_tag([from, to], Query1),
  QueryRealtime = Query2,
  QueryHistory1 = pulsedb_query:add_tag({from, Now-360}, QueryRealtime),
  QueryHistory2 = pulsedb_query:add_tag({to, Now-4}, QueryHistory1),
  {Name,
   pulsedb_query:render(QueryRealtime),
   pulsedb_query:render(QueryHistory2)}.


resolve_embed(Embed) ->
  case pulsedb_embed_resolver:resolve(Embed) of
    {ok, Data} -> 
      Json = jsx:decode(Data),
      Title = proplists:get_value(<<"title">>, Json, <<>>),
      Queries = proplists:get_value(<<"queries">>, Json, []),
      {ok, Title, Queries};
    Other ->
      Other
  end.