-module(pulsedb_disk).
-author('Max Lapshin <max@maxidoors.ru>').

-include("pulsedb.hrl").


-record(source, {
  id :: non_neg_integer(),
  name :: source_name(),
  original_name :: source_name(),
  original_tags :: list(),
  offsets_offset :: non_neg_integer(),
  block_offsets = [] :: non_neg_integer()
}).

-type source() :: #source{}.


-record(disk_db, 
 {
  storage = pulsedb_disk,
  path :: file:filename(),
  date :: binary() | undefined,
  sources :: [source()],
  cached_source_names = [],

  mode :: undefined | read | append,

  config_fd :: file:fd(),
  data_fd :: file:fd(),

  write_delay,
  buffer = [],
  
  config :: storage_config()
}).

-type chunk_entry() :: {Chunk :: non_neg_integer(), Offset :: non_neg_integer(), Length :: non_neg_integer()}.

-callback required_chunks(From :: utc(), To :: utc(), Date :: utc(), Config :: storage_config()) -> Chunks :: list(chunk_entry()).
-callback required_partitions(UTC :: utc(), UTC :: utc(), Config :: storage_config()) -> PathList :: list(file:filename()).

-callback chunk_number(UTC :: utc(), Config :: storage_config()) -> Num :: non_neg_integer().
-callback tick_number (UTC :: utc(), Config :: storage_config()) -> Num :: non_neg_integer().

-callback block_path(UTC :: utc()) -> Path :: file:filename().
-callback parse_date(Path :: file:filename()) -> UTC :: utc().

-callback block_start_utc(UTC :: utc(), Config :: storage_config()) -> FirstBlockUTC :: utc().
-callback block_end_utc  (UTC :: utc(), Config :: storage_config()) -> LastBlockUTC :: utc().

-callback last_day(Path :: file:filename()) -> Path :: file:filename()|undefined.



-export([open/2, append/2, read/3, close/1, sync/1]).
-export([info/1, full_info/2]).
-export([delete_older/2]).
-export([metric_name/2, metric_fits_query/2]).

-export([read_all/4]).


-spec open(Path::file:filename(), Options::list()) -> {ok, pulsedb:db()} | {error, Reason::any()}.
open(Path, Options) when is_list(Path) ->
  open(iolist_to_binary(Path), Options);
open(<<"file://", Path/binary>>, Options) ->
  open(Path, Options);
open(Path, Options) when is_binary(Path) ->
  Resolution = proplists:get_value(resolution, Options, seconds),
  Config = resolution_config(Resolution),
  WriteDelay = case proplists:get_value(write_delay, Options) of
    true -> 2000;
    WriteDelayCount when is_integer(WriteDelayCount) -> WriteDelayCount;
    _ -> undefined
  end,
  {ok, #disk_db{path = Path, config = Config, write_delay = WriteDelay}}.


resolution_config(minutes) ->
  #storage_config{
   ticks_per_chunk = 24*60,
   chunks_per_metric = 32,
   chunk_bits = 12,
   utc_step = 60,
   partition_module = pulsedb_disk_minutes
  };

resolution_config(_) ->
  #storage_config{
   ticks_per_chunk = 60*60,
   chunks_per_metric = 24,
   chunk_bits = 13,
   utc_step = 1,
   partition_module = pulsedb_disk_seconds
  }.


open0(#disk_db{path = Path, config_fd = undefined, date = Date} = DB, Mode) when Date =/= undefined ->
  case filelib:is_regular(filename:join([Path,Date,config_v4])) of
    true ->
      try open_existing_db(DB#disk_db{mode = Mode})
      catch
        Class:Error ->
          Trace = erlang:get_stacktrace(),
          lager:log(error,[{module,?MODULE},{line,?LINE}],"Error in pulsedb: ~p:~p, need to recover for date: ~p\n~p\n", [Class, Error, Date, Trace]),
          file:delete(filename:join([Path,Date,config_v4])),
          file:delete(filename:join([Path,Date,data_v4])),
          erlang:raise(Class, Error, Trace)
      end;
    false when Mode == read ->
      DB#disk_db{mode = read};
    false when Mode == append ->
      create_new_db(DB#disk_db{mode = append})
  end.


create_new_db(#disk_db{path = Path, date = Date, mode = append} = DB) when Date =/= undefined ->
  ConfigPath = filename:join([Path, Date, config_v4]),
  DataPath = filename:join([Path, Date, data_v4]),

  case filelib:ensure_dir(ConfigPath) of
    ok -> ok;
    {error, Reason1} -> throw({error, {create_path_failed,ConfigPath,Reason1}})
  end,

  Opts = [binary,write,exclusive,raw],

  {ok, ConfigFd} = case file:open(ConfigPath, Opts) of
    {ok, CFile_} -> {ok, CFile_};
    {error, Reason2} -> throw({error,{open_config_failed,ConfigPath,Reason2}})
  end,

  {ok, DataFd} = file:open(DataPath, Opts),

  DB#disk_db{path = Path, config_fd = ConfigFd, data_fd = DataFd, sources = []}.


read_file(Path) ->
  case file:read_file(Path) of
    {ok, Bin} -> Bin;
    {error, _} -> <<>>
  end.

open_existing_db(#disk_db{path = Path, date = Date, mode = Mode, config = Config} = DB) when Date =/= undefined, Mode =/= undefined ->
  ConfigPath = filename:join([Path, Date, config_v4]),
  DataPath = filename:join([Path, Date, data_v4]),

  DB1 = case DB#disk_db.sources of
    undefined ->
      DB#disk_db{sources = decode_config(read_file(ConfigPath), Config)};
    _ ->
      DB
  end,

  Opts = case Mode of
    append -> [binary,read,write];
    read -> [binary,read]
  end,

  {ok, ConfigFd} = file:open(ConfigPath, Opts),
  {ok, DataFd} = file:open(DataPath, Opts),

  DB1#disk_db{config_fd = ConfigFd, data_fd = DataFd}.




sync(#disk_db{config_fd = ConfigFd, data_fd = DataFd, buffer = Buffer} = DB) ->
  file:pwrite(DataFd, Buffer),
  case ConfigFd of
    undefined -> ok;
    _ -> file:sync(ConfigFd)
  end,
  case DataFd of
    undefined -> ok;
    _ -> file:sync(DataFd)
  end,
  {ok, DB#disk_db{buffer = []}}.


-spec append(pulsedb:tick() | [pulsedb:tick()], pulsedb:db()) -> pulsedb:db().

append([{_,_,_,_} = Tick|Ticks], #disk_db{} = DB) ->
  {ok, DB1} = append(Tick, DB),
  append(Ticks, DB1);

append([], #disk_db{} = DB) ->
  {ok, DB};

append(_, #disk_db{mode = read, path = Path}) ->
  error({need_to_reopen_pulsedb_for_append,Path});

append({Name, UTC, Value, _Tags} = T, #disk_db{}) when not is_binary(Name); not is_integer(UTC); not is_integer(Value) ->
  error({invalid_tick, T});

append({_Name, UTC, _Value, _Tags} = Tick, #disk_db{config_fd = undefined, date = undefined, 
                                                    config = #storage_config{partition_module=Partition}} = DB) ->
  append(Tick, open0(DB#disk_db{date = Partition:block_path(UTC)}, append));

append({_, UTC, _, _} = Tick, #disk_db{mode = append, date = Date, path = Path,
                                       config = #storage_config{partition_module=Partition}} = DB) ->
  UTCDate = Partition:block_path(UTC),
  {ok, DB1} = if
    Date == undefined orelse UTCDate == Date ->
      try append0(Tick, DB)
      catch
        Class:Error ->
          Trace = erlang:get_stacktrace(),
          lager:log(error,[{module,?MODULE},{line,?LINE}],"Error in pulsedb: ~p:~p, need to recover for date: ~s\n~p\n", [Class, Error, UTCDate, Trace]),
          file:delete(filename:join([Path,UTCDate,config_v4])),
          file:delete(filename:join([Path,UTCDate,data_v4])),
          erlang:raise(Class, Error, Trace)
      end;
    true ->
      {ok, DB_} = close(DB),
      append(Tick, DB_)
  end,
  {ok, DB1}.



append0({Name, UTC, Value, Tags}, #disk_db{} = DB) ->
  {ok, SourceId, DB1} = find_or_open_source(Name, Tags, DB),
  {ok, DB2} = append_data(SourceId, UTC, Value, DB1),
  {ok, DB2}.


find_or_open_source(Name, Tags, #disk_db{sources = S} = DB) ->
  {SourceName, DB1} = source_name(Name, Tags, DB),
  case lists:keyfind(SourceName, #source.name, S) of
    false -> append_new_source(SourceName, Name, Tags, DB1);
    #source{id = SourceId} -> {ok, SourceId, DB1}
  end.


source_name(Name, Tags, #disk_db{cached_source_names = SourceNames} = DB) ->
  case lists:keyfind({Name,Tags}, 1, SourceNames) of
    {_, SourceName} ->
      {SourceName, DB};
    false ->
      Sec = erlang:system_time(seconds),
      Cached =  length(SourceNames),
      SourceName = metric_name(Name, Tags),
      CachedSourceNames = if Sec rem 10 == 0 andalso Cached > 100 -> [{{Name,Tags},SourceName}] ++ lists:sublist(SourceNames, Cached div 2);
        true -> [{{Name,Tags},SourceName}] ++ SourceNames
      end,
      {SourceName, DB#disk_db{cached_source_names = CachedSourceNames}}
  end.



append_new_source(SourceName, Name, Tags, #disk_db{sources = Sources, config_fd = ConfigFd, config = Config} = DB) ->
  {ok, ConfigPos} = file:position(ConfigFd, eof),
  Source = #source{id = length(Sources), name = SourceName, offsets_offset = ConfigPos + 5 + size(SourceName),
    original_name = Name, original_tags = Tags},
  Bin = encode_config(Source, Config),
  ok = file:pwrite(ConfigFd, ConfigPos, Bin),
  {ok, Source#source.id, DB#disk_db{sources = Sources ++ [Source]}}.


-define(CONFIG_SOURCE, 4).

encode_config(#source{name = Name, block_offsets = Offsets}, #storage_config{}=Config) ->
  L = size(Name),
  OffsetsBin = pack_offsets(Offsets, Config),
  Conf = [<<L:16, Name:L/binary>>, OffsetsBin],
  Size = iolist_size(Conf),
  iolist_to_binary([<<?CONFIG_SOURCE, Size:16>>, Conf]).


pack_offsets(Offsets, #storage_config{chunks_per_metric=NChunks, offset_bytes=OSize}) ->
  OffsetSize = OSize * 8,
  [<<(proplists:get_value(I, Offsets, -1)):OffsetSize/signed>> || I <- lists:seq(0,NChunks-1)].

-spec decode_config(binary(), storage_config()) -> [source()].

decode_config(Bin, #storage_config{}=Config) when is_binary(Bin) ->
  decode_config(Bin, 0, 0, Config).

decode_config(<<?CONFIG_SOURCE, Length:16, Source:Length/binary, Rest/binary>>, ConfigOffset, Id, 
              #storage_config{chunks_per_metric=NChunks, offset_bytes=OSize}=Config) ->
  OffsetDataSize = NChunks * OSize,
  OffsetSize = OSize * 8,
  <<L:16, Name:L/binary, OffsetsBin:OffsetDataSize/binary>> = Source,
  Offsets =[ {I,O} || {I,O} <- lists:zip(lists:seq(0,NChunks-1), [O || <<O:OffsetSize/signed>> <= OffsetsBin]), O >= 0],

  [OriginalName|Tags] = binary:split(Name, <<":">>, [global]),
  OriginalTags = lists:map(fun(T) ->
    [K,V] = binary:split(T, <<"=">>),
    {K,V}
  end, Tags),
  [#source{id = Id, name = Name, block_offsets = Offsets, offsets_offset = ConfigOffset + 1 + 2 + 2 + L,
    original_name = OriginalName, original_tags = OriginalTags}
    |decode_config(Rest, ConfigOffset + 1 + 2 + Length, Id + 1, Config)];

decode_config(<<>>, _, _, _) ->
  [].



metric_name(Name, Tags) ->
 iolist_to_binary([to_b(Name), [[":",to_b(K),"=",to_b(V)] ||  {K,V} <- lists:sort(Tags)]]).

to_b(Atom) when is_atom(Atom) -> atom_to_binary(Atom, latin1);
to_b(Bin) when is_binary(Bin) -> Bin.

append_data(SourceId, UTC, Value, #disk_db{data_fd = DataFd, write_delay = WriteDelay, buffer = Buffer} = DB) ->
  #disk_db{sources = Sources, 
           config = #storage_config{partition_module = Partition,
                                    chunk_bits = ChunkBits,
                                    tick_bytes = TickSize}=Config} = DB1 = open_hour_if_required(SourceId, UTC, DB),
  #source{block_offsets = Offsets} = lists:keyfind(SourceId,#source.id,Sources),
  
  Chunk = Partition:chunk_number(UTC, Config),
  Tick = Partition:tick_number(UTC, Config),
  {_, ChunkOffset} = lists:keyfind(Chunk, 1, Offsets),
  Offset = (ChunkOffset bsl ChunkBits) + Tick*TickSize,

  case WriteDelay of
    undefined ->
      ok = file:pwrite(DataFd, Offset, pulsedb_data:shift_value(Value)),
      {ok, DB1};
    _ ->
      Buffer1 = [{Offset,pulsedb_data:shift_value(Value)}|Buffer],
      case length(Buffer1) of
        Len when Len > WriteDelay -> 
          file:pwrite(DataFd, collapse(Buffer1)),
          {ok, DB1#disk_db{buffer = []}};
        _ ->
          {ok, DB1#disk_db{buffer = Buffer1}}
      end
  end.


collapse(Buffer) ->
  collapse0(lists:sort(Buffer)).

collapse0([{Offset1,Bin1},{Offset2,Bin2}|Chunks]) when Offset1 + size(Bin1) == Offset2 ->
  collapse0([{Offset1, <<Bin1/binary, Bin2/binary>>}|Chunks]);

collapse0([{Offset,Bin}|Chunks]) -> [{Offset,Bin}|collapse0(Chunks)];
collapse0([]) -> [].



open_hour_if_required(SourceId, UTC, #disk_db{config_fd = ConfigFd, data_fd = DataFd, sources = Sources, 
                                              config = #storage_config{ticks_per_chunk=NTicks, tick_bytes=TickSize,
                                                                       chunk_bits=ChunkBits, partition_module=Partition}=Config} = DB) ->
  ChunkSize = 1 bsl ChunkBits,
  ChunkTicksSize = TickSize * NTicks,
  ChunkStuffSize = ChunkSize - ChunkTicksSize,
  ChunkMaxInfoSize = ChunkStuffSize - 1,
  ChunkNo = Partition:chunk_number(UTC, Config),
  
  #source{block_offsets = Offsets, name = Name, offsets_offset = O} = Source = lists:keyfind(SourceId,#source.id,Sources),
  case lists:keyfind(ChunkNo, 1, Offsets) of
    {_, Offset} when is_number(Offset) andalso Offset >= 0 ->
      DB;
    _ ->
      {ok, DataPos} = file:position(DataFd, eof),
      BlockOffset = case DataPos rem ChunkSize of
        0 ->
          DataPos;
        _ ->
          lager:error("Error with data file for ~p / ~p. Last offset is ~p, chunk size: ~p", [DB#disk_db.path, DB#disk_db.date, DataPos, ChunkSize]),
          ((DataPos div ChunkSize) + 1)*ChunkSize
      end,
      AdditionalInfo = case iolist_to_binary([Partition:block_path(UTC)," ", integer_to_list(ChunkNo), " ", Name]) of
        <<Info:ChunkMaxInfoSize/binary, _/binary>> -> <<Info/binary, 0>>;
        Info -> [Info, binary:copy(<<0>>, ChunkStuffSize - size(Info))]
      end,
      ok = file:pwrite(DataFd, BlockOffset + ChunkTicksSize, AdditionalInfo),
      Offsets1 = lists:keystore(ChunkNo, 1, Offsets, {ChunkNo, BlockOffset bsr ChunkBits}),
      ok = file:pwrite(ConfigFd, O, pack_offsets(Offsets1, Config)),

      Sources1 = lists:keystore(SourceId, #source.id, Sources, Source#source{block_offsets = Offsets1}),
      DB#disk_db{sources = Sources1}
  end.






info(#disk_db{sources = Sources}) when is_list(Sources) ->
  Src = lists:sort([{Name,lists:sort(Tags)} || #source{original_name = Name, original_tags = Tags} <- Sources]),
  [{sources,Src}];

info(#disk_db{path = Path, config=#storage_config{partition_module=Partition}=Config} = DB) ->
  case Partition:last_day(Path) of
    undefined -> [];
    DayPath -> info(DB#disk_db{sources = decode_config(read_file(filename:join(DayPath,config_v4)), Config)})
  end.


full_info(Date, #disk_db{path = DbPath, 
                    config = #storage_config{ticks_per_chunk=NTicks, 
                                             chunks_per_metric=NChunks,
                                             chunk_bits=ChunkBits,
                                             utc_step = UTCStep,
                                             partition_module=Partition}=Config}) ->
  UTC = pulsedb_time:parse(Date),
  Path = Partition:block_path(UTC),
  DateUTC = Partition:parse_date(Date),
  DataPath = filename:join([DbPath, Path]),
  Sources0 = decode_config(read_file(filename:join(DataPath,config_v4)), Config),
  Sources = lists:keysort(#source.original_name, Sources0),

  MapSource = 
  fun (#source{original_name = Name, original_tags = Tags, block_offsets = BOs}) ->
    Chunks = [[{index, N},
               {utc, DateUTC + N*NTicks*UTCStep},
               {offset, Offset bsl ChunkBits}]
              || {N,Offset} <- BOs],
    [{name, Name},
     {tags, lists:sort(Tags)},
     {chunks, Chunks}]
  end,
  
  [{utc, DateUTC},
   {path, DataPath},
   {chunks_per_metric, NChunks},
   {ticks_per_chunk, NTicks},
   {seconds_per_tick, UTCStep},
   {chunk_size, 1 bsl ChunkBits},
   {sources, [MapSource(S) || S <- Sources]}].




-spec read(Name::source_name(), Query::[{binary(),binary()}], pulsedb:db()) -> {ok, [tick()], pulsedb:db()} | {error, Reason::any()}.

% read(_Name, _Query, #disk_db{mode = append}) ->
%   {error, need_to_reopen_for_read};

read(Name, Query, #disk_db{path = Path, date = Date, config = #storage_config{partition_module=Partition}=Config} = DB) ->
  {from,From} = lists:keyfind(from,1,Query),
  {to,To} = lists:keyfind(to,1,Query),
  RequiredDates = Partition:required_partitions(From, To, Config),
  case load_ticks(RequiredDates, Name, Query, #disk_db{path = Path, date = Date, config = Config}) of
    {ok, Ticks, DB1} ->
      close(DB1),
      {ok, Ticks, DB};
    {error, _} = Error ->
      Error
  end.

% -> [{{Name, Tags}, Ticks}]
read_all(From, To, Opts, #disk_db{config = #storage_config{partition_module=Partition}=Config} = DB) ->
  RequiredDates = Partition:required_partitions(From, To, Config),
  Data = 
  [begin
     DB1 = open_existing_db(DB#disk_db{date = Date, mode = read}),
     SourceData = try
       Info = pulsedb:info(DB1),
       Sources = proplists:get_value(sources, Info),
       [begin
          Query = [{from, From}, {to, To}|Tags]++Opts,
          {ok, Ticks, _} = read0(Name, Query, DB1),
          {Source, Ticks}
          end || {Name, Tags}=Source <- Sources]
     catch _:_ -> []
     end,
     close(DB1),
     SourceData
   end
   || Date <- RequiredDates],
  {ok, lists:concat(Data), DB}.



load_ticks([], _Name, _Query, DB) ->
  {ok, [], DB};

load_ticks([Date|Dates], Name, Query, #disk_db{} = DB) ->
  case read0(Name, Query, DB#disk_db{date = Date}) of
    {ok, Ticks1, DB1} ->
      {ok, DB2} = close(DB1),
      case load_ticks(Dates, Name, Query, DB2) of
        {ok, Ticks2, DB3} ->
          {ok, Ticks1++Ticks2, DB3};
        {error, _} = Error ->
          Error
      end;
    {error, _} = Error ->
      Error
  end.





read0(Name, Query, #disk_db{config_fd = undefined, date = Date} = DB) when Date =/= undefined ->
  case open0(DB, read) of
    #disk_db{config_fd = undefined} = DB1 ->
      {ok, [], DB1};
    #disk_db{} = DB1 ->
      read0(Name, Query, DB1)
  end;

read0(Name, Query, #disk_db{sources = Sources, data_fd = DataFd, date = Date, mode = read, 
                            config = #storage_config{ticks_per_chunk=NTicks, 
                                                     tick_bytes=TickSize, 
                                                     chunk_bits=ChunkBits,
                                                     utc_step = UTCStep,
                                                     partition_module=Partition} = Config} = DB) when Date =/= undefined ->
  Self = self(),
  Tags = [{K,V} || {K,V} <- Query, is_binary(K)],
  
  ReadSources = select_sources(Name, Tags, Sources),
  DateUTC = Partition:parse_date(Date),
  
  From = proplists:get_value(from,Query, Partition:block_start_utc(DateUTC, Config)),
  To = proplists:get_value(to,Query, Partition:block_end_utc(DateUTC, Config)),
  
  HrsToRead = Partition:required_chunks(From, To, DateUTC, Config),
  
  Ticks1 = lists:flatmap(fun({H,TickOffset,Limit}) ->
    Ref = make_ref(),
                           
    ReadFn = fun() ->
      Ticks2 = lists:map(fun(#source{block_offsets = Offsets}) ->
        case lists:keyfind(H, 1, Offsets) of
          {ChunkN, Offset} ->
            case file:pread(DataFd, Offset bsl ChunkBits + TickSize*TickOffset, TickSize*Limit) of
              {ok, Bin} ->
                StartUTC = DateUTC + (ChunkN*NTicks + TickOffset)*UTCStep,
                RawTicks = pulsedb_data:unpack_ticks(Bin, StartUTC, UTCStep),
                pulsedb_data:interpolate(StartUTC, Limit, UTCStep, RawTicks);
              _ ->
                []
            end;
          false ->
            []
        end
      end, ReadSources),
      Ticks4 = aggregate(proplists:get_value(aggregator,Query), [L || L <- Ticks2, L =/= []]),
      Self ! {chunk_data, Ref, Ticks4}
    end,

    ReaderPid = spawn(ReadFn),
    receive
      {chunk_data, Ref, ChunkTicks} ->
        ensure_dead(ReaderPid),
        ChunkTicks
    after
      20000 ->
        ensure_dead(ReaderPid),
        lager:info("db reader timeout ~p", [{Name, Date, H, process_info(self(), messages)}]),
        []
    end
  end, HrsToRead),

  Ticks5 = pulsedb_data:downsample(proplists:get_value(downsampler,Query), Ticks1),
  {ok, Ticks5, DB}.

       
aggregate(_,[]) -> [];
aggregate(_,[[]|_]) -> [];
aggregate(Agg, List1) ->
  {List2, Heads} = lists:mapfoldl(fun( [{_, H}|T], Acc) ->  {T, [H|Acc]}  end, [], List1), 
  {Utc,_} = hd(hd(List1)),
  Result = pulsedb_data:aggregate_values(Agg, Heads),
  [{Utc, Result} | aggregate(Agg, List2) ].

ensure_dead(Pid) ->
  exit(Pid, kill).



metric_fits_query(Query, Tags) ->
  [] == [K || {K,V} <- Query, proplists:get_value(K,Tags) =/= V].



select_sources(_Name, _Tags, []) ->
  [];
select_sources(Name, Tags, [#source{original_name = Name, original_tags = Tags1} = S|Sources]) ->
  case metric_fits_query(Tags, Tags1) of
    true -> [S|select_sources(Name, Tags, Sources)];
    _ -> select_sources(Name, Tags, Sources)
  end;

select_sources(Name, Tags, [_|Sources]) ->
  select_sources(Name, Tags, Sources).







-spec close(pulsedb:db()) -> {pulsedb:db()}.
close(#disk_db{config_fd = C, data_fd = D} = DB) ->
  file:close(C),
  file:close(D),
  {ok, DB#disk_db{config_fd = undefined, data_fd = undefined,
    date = undefined, sources = undefined}}.




delete_older(Time, #disk_db{path = Path, config = #storage_config{partition_module = Partition}} = DB) ->
  {Now, _} = pulsedb:current_second(),
  GoodDate = binary_to_list(Partition:block_path(Now - Time)),
  Dates = [Date || Date <- filelib:wildcard("*/*/*", binary_to_list(Path)), Date < GoodDate],

  [begin
    file:delete(filename:join([Path,Date,config_v4])),
    file:delete(filename:join([Path,Date,data_v4])),
    file:del_dir(filename:join(Path,Date)),
    file:del_dir(filename:join(Path,filename:dirname(Date))),
    file:del_dir(filename:join(Path,filename:dirname(filename:dirname(Date)))),
    ok
  end || Date <- Dates],

  {ok, DB}.






