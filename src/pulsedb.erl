-module(pulsedb).

-include("pulsedb.hrl").


-export_types([db/0, utc/0, source_name/0, tick/0]).


-export([open/1, open/2, append/2, read/3, read/2, close/1, sync/1, read_once/2]).
-export([info/1, parse_query/1]).

-export([collect/3, collect/4, stop_collector/1]).
-export([current_second/0, current_minute/0, current_hour/0]).

-export([subscribe/2, unsubscribe/1]).
-export([replicate/1]).


-spec open(Path::file:filename()) -> {ok, pulsedb:db()} | {error, Reason::any()}.
open(seconds) -> {ok, seconds};
open(minutes) -> {ok, minutes};

open(Path) ->
  open(Path, []).


-spec open(Path::file:filename()|atom(), Options::list()) -> {ok, pulsedb:db()} | {error, Reason::any()}.
open(Path, Options) when is_list(Path) ->
  open(undefined, [{url, <<"file://", (list_to_binary(Path))/binary>>}|Options]);

open(Path, Options) when is_binary(Path) ->
  open(undefined, [{url, <<"file://", Path/binary>>}|Options]);

open(Name, Path) when is_atom(Name), is_binary(Path) ->
  open(Name, [{url, <<"file://", Path/binary>>}]);

open(Name, Path) when is_atom(Name), is_list(Path) ->
  open(Name, [{url, <<"file://", (list_to_binary(Path))/binary>>}]);

open(Name, Options) when is_atom(Name) ->
  case Name of
    undefined ->
      {value, {url, URL_}, Opts1} = lists:keytake(url, 1, Options),
      URL = iolist_to_binary(URL_),
      case URL of
        <<"file://", _/binary>> -> pulsedb_disk:open(URL, Opts1);
        <<"pulse://", _/binary>> -> pulsedb_netpush:open(URL, Opts1);
        <<"pulses://", _/binary>> -> pulsedb_netpush:open(URL, Opts1);
        <<"sharded://", _/binary>> -> pulsedb_sharded:open(URL, Opts1)
      end;
    _ -> 
      pulsedb_worker:start_link(Name, Options)
  end.


-spec append(tick() | [tick()], db()) -> db().
append(Ticks0, DB) ->
  Ticks = validate(Ticks0),
  if
    is_pid(DB) -> 
      pulsedb_worker:append(Ticks, DB);
    DB == seconds orelse DB == minutes ->
      pulsedb_memory:append(Ticks, DB);
    is_atom(DB) ->
      pulsedb_worker:append(Ticks, DB);    
    is_tuple(DB) ->
      Module = element(2,DB),
      Module:append(Ticks, DB)
  end.


validate([Tick|Ticks]) ->
  [validate(Tick)|validate(Ticks)];
validate([]) ->
  [];
validate({Name,UTC,Value,Tags} = Tick) ->
  is_binary(Name) orelse is_atom(Name) orelse error({wrong_name,Tick}),
  is_integer(UTC) andalso UTC >= 0 andalso UTC =< 4294967295 orelse error({wrong_utc,Tick}),
  is_list(Tags) orelse error({wrong_tags,Tick}),

  Tags1 = [make_tag(T) || {_,_}=T <- Tags],
  V1 = if
    Value < 0 -> 0;
    true -> Value
  end,
  {to_b(Name),UTC,V1,Tags1}.

make_tag({K,V}=T) when is_binary(K), is_binary(V) -> T;
make_tag({aggregator,V}) -> {aggregator, to_b(V)};
make_tag({K,V}) -> {to_b(K), to_b(V)}.
   



-spec close(pulsedb:db()) -> {ok, pulsedb:db()}.
close(seconds) -> ok;
close(minutes) -> ok;
close(DB) ->
  if
    is_pid(DB) -> pulsedb_worker:stop(DB);
    is_atom(DB) -> pulsedb_worker:stop(DB);
    is_tuple(DB) -> (element(2,DB)):close(DB)
  end.



-spec sync(pulsedb:db()) -> {ok, pulsedb:db()}.
sync(DB) ->
  if
    is_pid(DB) -> pulsedb_worker:sync(DB);
    is_atom(DB) -> pulsedb_worker:sync(DB);
    is_tuple(DB) -> (element(2,DB)):sync(DB)
  end.



subscribe(Query, Tag) ->
  pulsedb_realtime:subscribe(Query, Tag).

unsubscribe(Ref) ->
  pulsedb_realtime:unsubscribe(Ref).



replicate(DB) when DB == seconds orelse DB == minutes ->
  pulsedb_memory:replicate(DB, self()).


% -export([open/1, read/2, info/1, append/2, merge/2, close/1]).





-spec read(Name::source_name(), Query::[{atom(),any()}], pulsedb:db()) -> {ok, [tick()], pulsedb:db()} | {error, Reason::any()}.
read(Name, Query, DB) ->
  Query1 = clean_query(Query),
  Downsampler = proplists:get_value(downsampler, Query),
  if
    is_pid(DB) -> pulsedb_worker:read(Name, Query1, DB);
    DB == memory andalso Downsampler == {60,<<"avg">>} -> pulsedb_memory:read(Name, Query1, minutes);
    DB == memory -> pulsedb_memory:read(Name, Query1, seconds);
    DB == seconds orelse DB == minutes -> pulsedb_memory:read(Name, Query1, DB);
    is_atom(DB) -> pulsedb_worker:read(Name, Query1, DB);
    is_tuple(DB) -> (element(2,DB)):read(Name, Query1, DB)
  end.


read(Query0, DB) when is_list(Query0) ->
  read(iolist_to_binary(Query0), DB);

read(Query0, DB) when is_binary(Query0) ->
  {Aggregator, Downsampler, Name, Query} = parse_query(Query0),
  read(Name, [{aggregator,Aggregator},{downsampler,Downsampler}] ++ Query, DB).


read_once(Query0, DBOpts0) when is_list(DBOpts0) ->
  {Aggregator, Downsampler, Name0, Query1} = parse_query(Query0),
  try
    Resolution = 
    case Downsampler of
      undefined -> seconds;
      {Interval,_} when Interval < 60          -> seconds;
      %{Interval,_} when Interval rem 3600 == 0 -> hours;
      {Interval,_} when Interval rem 60 == 0   -> minutes;
      _ -> throw({incorrect_downsampler, Downsampler})
    end,

    Name = 
    case Downsampler of
      undefined         -> Name0;
      {I,_} when I < 60 -> Name0;
      {_,<<"avg">>}     -> Name0;

      {_,DS} when is_binary(DS) -> 
        <<DS/binary, "-", Name0/binary>>;

      {_,DS} when is_binary(DS) ->
        Prefix = iolist_to_binary(lists:concat([DS, "-"])),
        <<Prefix/binary, Name0/binary>>;
      _ -> throw({incorrect_downsampler, Downsampler})
    end,

    DBOpts = [{resolution, Resolution}|proplists:delete(Resolution, DBOpts0)],
    {ok, DB0} = open(undefined, DBOpts),
    Query = [{aggregator,Aggregator},{downsampler,Downsampler}] ++ clean_query(Query1),
    {ok, Ticks, DB1} = read(Name, Query, DB0),
    close(DB1),
    {ok, Ticks}
  catch throw:E -> 
    {error, E}
  end.
    
  
  


parse_query(Query) ->
  pulsedb_parser:parse(Query).


clean_query([{from,From}|Query]) ->  [{from,pulsedb_time:parse(From)}|clean_query(Query)];
clean_query([{to,To}|Query])     ->  [{to,pulsedb_time:parse(To)}|clean_query(Query)];
clean_query([{aggregator,A}|Query])->[{aggregator,A}|clean_query(Query)];
clean_query([{downsampler,D}|Query])->[{downsampler,D}|clean_query(Query)];
clean_query([{Key,Value}|Query]) ->  [{to_b(Key),Value}|clean_query(Query)];
clean_query([])                  ->  [].


to_b(Atom) when is_atom(Atom) -> atom_to_binary(Atom, latin1);
to_b(Bin) when is_binary(Bin) -> Bin.



collect(Name, Module, Args) ->
  collect(Name, Module, Args, []).

collect(Name, Module, Args, Options) ->
  pulsedb_sup:start_collector(Name, Module, Args, Options).

stop_collector(Name) ->
  pulsedb_collector:stop(Name).

% -spec merge([pulsedb:tick()], pulsedb:db()) -> {ok, pulsedb:db()} | {error, Reason::any()}.

% merge([], #db{} = DB) ->
%   {ok, 0, DB};

% merge([#tick{utc = FirstUTC, name = Name}|_] = Ticks, #db{path = Path} = DB) ->
%   {ok, RDB0} = open(Path),
%   From = pulsedb_time:parse(FirstUTC),
%   To = From+86400,
%   {ok, AvailTicks, RDB1} = read([{from,From},{to,To},{name,Name}], RDB0),
%   close(RDB1),
%   WriteTicks = [Tick || #tick{utc = UTC} = Tick <- Ticks, not lists:keymember(UTC, #tick.utc, AvailTicks)],
%   {ok, DB1} = pulsedb_disk:append(WriteTicks, DB),
%   {ok, length(WriteTicks), DB1}.





-spec info(pulsedb:db()|file:filename()) -> term().
info(DB) when is_tuple(DB) ->
  (element(2,DB)):info(DB);

info(DB) when DB == seconds orelse DB == minutes ->
  pulsedb_memory:info(DB);

info(Atom) when is_atom(Atom) ->
  pulsedb_worker:info(Atom);

info(Path) when is_binary(Path) ->
  {ok, DB} = pulsedb_disk:open(Path, []),
  Info = pulsedb_disk:info(DB),
  pulsedb_disk:close(DB),
  Info.





%%
%% Here goes common API

-spec current_second() -> {Number::non_neg_integer(), DelayTillNextSecond::non_neg_integer()}.
current_second() ->
  {Mega, Sec, Micro} = os:timestamp(),
  Milli = Micro div 1000,
  Delay = 500 + 1000 - Milli,
  {Mega*1000000 + Sec, Delay}.


-spec current_minute() -> {Number::non_neg_integer(), DelayTillNext::non_neg_integer()}.
current_minute() ->
  {Mega, Sec, Micro} = os:timestamp(),
  Milli = Micro div 1000,
  Second = Mega*1000000 + Sec,
  Delay = (90 - (Second rem 60))*1000 + Milli,
  {Second, Delay}.

current_hour() ->
  {Mega, Sec, Micro} = os:timestamp(),
  Milli = Micro div 1000,
  Second = Mega*1000000 + Sec,
  Delay = (3900 - (Second rem 3600))*1000 + Milli,
  {Second, Delay}.

