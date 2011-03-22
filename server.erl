%% - Server module
%% - The server module creates a parallel registered process by spawning a process which 
%% evaluates initialize(). 
%% The function initialize() does the following: 
%%      1/ It makes the current process as a system process in order to trap exit.
%%      2/ It creates a process evaluating the store_loop() function.
%%      4/ It executes the server_loop() function.

-module(server).

-export([start/0]).

%%%%%%%%%%%%%%%%%%%%%%% STARTING SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start() -> 
    register(transaction_server, spawn(fun() ->
					       process_flag(trap_exit, true),
					       Val= (catch initialize()),
					       io:format("Server terminated with:~p~n",[Val])
				       end)).

initialize() ->
    process_flag(trap_exit, true),
    Initialvals = [{a,0},{b,0},{c,0},{d,0}], %% All variables are set to 0
    ObjectTimeStamps = [{a,0,0}, {b,0,0}, {c,0,0},{d,0,0}],  % {object, writetimestamp, readtimestamp}
    TransactionTimeStamps = [], %[{clientpid,transactionstimestamp,[dependencies],[OldObjects]},...]    
    CurrentTimeStamp = 0,
    Transactions = {CurrentTimeStamp, TransactionTimeStamps, ObjectTimeStamps},
    ServerPid = self(),
    StorePid = spawn_link(fun() -> store_loop(ServerPid,Initialvals) end),
    server_loop([],StorePid, Transactions).
%%%%%%%%%%%%%%%%%%%%%%% STARTING SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%% ACTIVE SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% - The server maintains a list of all connected clients and a store holding
%% the values of the global variable a, b, c and d 
server_loop(ClientList,StorePid, Transactions) ->
    receive
	{login, MM, Client} ->
	    MM ! {ok, self()},
	    io:format("New client has joined the server:~p.~n", [Client]),
	    StorePid ! {print, self()},
	    server_loop(add_client(Client,ClientList),StorePid, Transactions);
	{close, Client} -> 
	    io:format("Client~p has left the server.~n", [Client]),
	    StorePid ! {print, self()},
	    server_loop(remove_client(Client,ClientList),StorePid, Transactions);
	{request, Client} ->
	    NewTransactions = start_transaction(Client, Transactions),
	    Client ! {proceed, self()},
	    server_loop(ClientList,StorePid, NewTransactions);
	{confirm, Client} ->
	    Client ! {abort, self()},
	    server_loop(ClientList,StorePid, Transactions);
	{action, Client, Act} ->
	    io:format("Received~p from client~p.~n", [Act, Client]),
	    case valid_action(Act, Client, Transactions) of
		false -> Client ! {abort, self()},
			 NewTransactions = Transactions;
		true  ->
		    NewTransactions = update_transaction(Act, Client, Transactions)
	    end,

	    server_loop(ClientList,StorePid, NewTransactions)
    after 50000 ->
	case all_gone(ClientList) of
	    true -> exit(normal);
	    false -> server_loop(ClientList,StorePid, Transactions)
	end
    end.

%% - The values are maintained here
store_loop(ServerPid, Database) -> 
    receive
	{print, ServerPid} -> 
	    io:format("Database status:~n~p.~n",[Database]),
	    store_loop(ServerPid,Database)
    end.
%%%%%%%%%%%%%%%%%%%%%%% ACTIVE SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_transaction(Client, {Clock, TimeStamps, ObjectTimeStamps}) ->
    NewClock = Clock + 1,
    {NewClock, [{Client, NewClock, [], []} | TimeStamps], ObjectTimeStamps}.

valid_action(Act, Client, {_,  TransactionTimeStamps, ObjectTimeStamps}) ->
    {value, {_, W, R}} = get_object_timestamp(ObjectTimeStamps, Act),
    {value, {_,Timestamp,_,_}} = get_transaction(TransactionTimeStamps, Client),
    case Act  of
	{read, _} ->
	    W > Timestamp;
	{write, _, _} ->
	    R > Timestamp andalso W > Timestamp
    end.

get_object_timestamp(ObjectTimeStamps, {write, O, _}) ->
    lists:keysearch(O, 1, ObjectTimeStamps).

get_transaction(TransactionTimeStamp, Client) ->
    lists:keysearch(Client, 1, TransactionTimeStamp).

update_transaction(_,_,Transaction) ->
    Transaction.

%% - Low level function to handle lists
add_client(C,T) -> [C|T].

remove_client(_,[]) -> [];
remove_client(C, [C|T]) -> T;
remove_client(C, [H|T]) -> [H|remove_client(C,T)].

all_gone([]) -> true;
all_gone(_) -> false.
