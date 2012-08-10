%%% @doc Stock database
%%% Designed for continious writing of stock data
%%% with later fast read and fast seek
-module(stockdb).
-author({"Danil Zagoskin", z@gosk.in}).
-include("../include/stockdb.hrl").
-include("log.hrl").
-include("stockdb.hrl").

%% Application configuration
-export([get_value/1, get_value/2]).

%% Querying available data
-export([stocks/0, stocks/1, dates/1, dates/2, common_dates/1, common_dates/2]).
%% Get information about stock/date file
-export([info/3]).

%% Writing DB
-export([open_append/3, append/2, close/1]).
%% Reading existing data
-export([open_read/2, events/1, events/2]).
%% Iterator API
-export([init_reader/2, init_reader/3, read_event/1]).

%% Run tests
-export([run_tests/0]).


%% @doc List of stocks in local database
-spec stocks() -> [stock()].
stocks() -> stockdb_fs:stocks().

%% @doc List of stocks in remote database
-spec stocks(Storage::term()) -> [stock()].
stocks(Storage) -> stockdb_fs:stocks(Storage).


%% @doc List of available dates for stock
-spec dates(stock()) -> [date()].
dates(Stock) -> stockdb_fs:dates(Stock).

%% @doc List of available dates in remote database
-spec dates(Storage::term(), Stock::stock()) -> [date()].
dates(Storage, Stock) -> stockdb_fs:dates(Storage, Stock).

%% @doc List dates when all given stocks have data
-spec common_dates([stock()]) -> [date()].
common_dates(Stocks) -> stockdb_fs:common_dates(Stocks).

%% @doc List dates when all given stocks have data, remote version
-spec common_dates(Storage::term(), [stock()]) -> [date()].
common_dates(Storage, Stocks) -> stockdb_fs:common_dates(Storage, Stocks).


%% @doc Open stock for reading
-spec open_read(stock(), date()) -> {ok, stockdb()} | {error, Reason::term()}.  
open_read(Stock, Date) ->
  stockdb_reader:open(stockdb_fs:path(Stock, Date)).

%% @doc Open stock for appending
-spec open_append(stock(), date(), [open_option()]) -> {ok, stockdb()} | {error, Reason::term()}.  
open_append(Stock, Date, Opts) ->
  stockdb_appender:open(stockdb_fs:path({proplists:get_value(type,Opts,stock), Stock}, Date), [{stock,Stock},{date,stockdb_fs:parse_date(Date)}|Opts]).

%% @doc Append row to db
-spec append(stockdb(), trade() | market_data()) -> {ok, stockdb()} | {error, Reason::term()}.
append(Stockdb, Event) ->
  stockdb_appender:append(Stockdb, Event).

%% @doc Fetch requested information about given Stock/Date
info(Stock, Date, Fields) ->
  stockdb_reader:file_info(stockdb_fs:path(Stock, Date), Fields).

%% @doc Read all events for stock and date
-spec events(stock(), date()) -> {ok, list(trade() | market_data())}.
events(Stock, Date) ->
  {ok, Iterator} = init_reader(Stock, Date, []),
  events(Iterator).

%% @doc Just read all events from stockdb
-spec events(stockdb()|iterator()) -> {ok, list(trade() | market_data())}.
events(#dbstate{} = Stockdb) ->
  {ok, Iterator} = init_reader(Stockdb, []),
  events(Iterator);

events(Iterator) ->
  stockdb_iterator:all_events(Iterator).

%% @doc Init iterator over opened stockdb
% Options: 
%    {range, Start, End}
%    {filter, FilterFun, FilterArgs}
% FilterFun is function in stockdb_filters
-spec init_reader(stockdb(), list(reader_option())) -> {ok, iterator()} | {error, Reason::term()}.
init_reader(Stockdb, Filters) ->
  {ok, Iterator} = stockdb_iterator:init(Stockdb),
  {ok, apply_filters(Iterator, Filters)}.

%% @doc Shortcut for opening iterator on stock-date pair
-spec init_reader(stock(), date(), list(reader_option())) -> {ok, iterator()} | {error, Reason::term()}.
init_reader(Stock, Date, Filters) ->
  {ok, Stockdb} = open_read(Stock, Date),
  init_reader(Stockdb, Filters).


apply_filter(Iterator, false) -> Iterator;
apply_filter(Iterator, {range, Start, End}) ->
  stockdb_iterator:set_range({Start, End}, Iterator);
apply_filter(Iterator, {filter, Function, Args}) ->
  stockdb_iterator:filter(Iterator, Function, Args).

apply_filters(Iterator, []) -> Iterator;
apply_filters(Iterator, [Filter|MoreFilters]) ->
  apply_filters(apply_filter(Iterator, Filter), MoreFilters).


%% @doc Read next event from iterator
-spec read_event(iterator()) -> {ok, trade() | market_data(), iterator()} | {eof, iterator()}.
read_event(Iterator) ->
  stockdb_iterator:read_event(Iterator).

%% @doc close stockdb
-spec close(stockdb()) -> ok.
close(#dbstate{file = F} = _Stockdb) ->
  case F of 
    undefined -> ok;
    _ -> file:close(F)
  end,
  ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%       Configuration
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc get configuration value with fallback to given default
get_value(Key, Default) ->
  case application:get_env(?MODULE, Key) of
    {ok, Value} -> Value;
    undefined -> Default
  end.

%% @doc get configuration value, raise error if not found
get_value(Key) ->
  case application:get_env(?MODULE, Key) of
    {ok, Value} -> Value;
    undefined -> erlang:error({no_key,Key})
  end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%       Testing
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

run_tests() ->
  eunit:test({application, stockdb}).

