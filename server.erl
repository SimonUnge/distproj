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
    TransactionTimeStamps = [],    %[{clientpid,transactionstimestamp,{ok, [dependencies]},[OldObjects]},...]    
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
	    %%CHECK DEPLISTS...
	    io:format("Transactions? ~p~n",[Transactions]),
	    NewTransactions = end_transaction(Client, Transactions),
	    server_loop(ClientList,StorePid, NewTransactions);
	{action, Client, Act} ->
	    io:format("Received~p from client~p.~n", [Act, Client]),
	    case valid_action(Act, Client, Transactions) of
		false -> 
		    %%ROLLBACK
		    io:format("FALSE"),
		    Client ! {abort, self()},
		    server_loop(ClientList,StorePid, Transactions);
		true  ->
		    io:format("Valid action"),
		    StorePid ! {Act, self()},
		    receive
			{Object, StorePid} ->
			    NewTransactions = update_transaction(Act, Client, Object, Transactions),
			    server_loop(ClientList,StorePid, NewTransactions)
		    end
	    end

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
	    store_loop(ServerPid,Database);
	{{write, O, V}, ServerPid} ->
	    {value, OldObj} = lists:keysearch(O,1,Database),
	    NewDatabase = lists:keyreplace(O, 1, Database, {O,V}),
	    ServerPid ! {OldObj, self()},
	    io:format("Database status after write:~n~p.~n",[NewDatabase]),
	    store_loop(ServerPid, NewDatabase);
	    
	{{read, O}, ServerPid} ->
	    {value, OldObj} = lists:keysearch(O,1,Database),
	    ServerPid ! {OldObj, self()},
	    io:format("Database status after read:~n~p.~n",[Database]),
	    store_loop(ServerPid, Database)
    end.

%%%%%%%%%%%%%%%%%%%%%%% ACTIVE SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_transaction(Client, {Clock, TimeStamps, ObjectTimeStamps}) ->
    NewClock = Clock + 1,
    {NewClock, [{Client, NewClock, {ok, []}, []} | TimeStamps], ObjectTimeStamps}.

valid_action(Act, Client, {_,  TransactionTimeStamps, ObjectTimeStamps}) ->
    {value, {_, W, R}} = get_object_timestamp(ObjectTimeStamps, Act),
    {value, {_,Timestamp,_,_}} = get_transaction(TransactionTimeStamps, Client),
    case Act  of
	{read, _} ->
	    W =< Timestamp;
	{write, _, _} ->
	    R =< Timestamp andalso W =< Timestamp
    end.

get_object_timestamp(ObjectTimeStamps, {write, O, _}) ->
    lists:keysearch(O, 1, ObjectTimeStamps);
get_object_timestamp(ObjectTimeStamps, {read, O}) ->
    lists:keysearch(O, 1, ObjectTimeStamps).

get_transaction(TransactionTimeStamp, Client) ->
    lists:keysearch(Client, 1, TransactionTimeStamp).

update_transaction(Act, Client, Object,{CurrentTimeStamp, TransactionTimeStamps, ObjectTimeStamps}) ->
    case Act of
	{read, O} ->
	    {value, {O, WTS, RTS}} = lists:keysearch(O, 1, ObjectTimeStamps),
	    {value, {_, TimeStamp, {Status, Deptlist}, Oldobj}} = get_transaction(TransactionTimeStamps, Client),
	     NewRTS = maxx(RTS, TimeStamp),
	     NewObjectTimeStamps = lists:keyreplace(O, 1, ObjectTimeStamps, {O, WTS, NewRTS}),
	     NewTransactionTimeStamps = lists:keyreplace(
					  Client,
					  1, 
					  TransactionTimeStamps, 
					  {Client, TimeStamp, {Status,[{O, WTS, RTS} | Deptlist]}, Oldobj}),

	     {CurrentTimeStamp, NewTransactionTimeStamps, NewObjectTimeStamps};
	
	{write, O, _} ->
	    {value, {O, WTS, RTS}} = lists:keysearch(O, 1, ObjectTimeStamps),
	    {value, {_, TimeStamp, Deptlist, Oldobj}} = get_transaction(TransactionTimeStamps, Client),
	    NewWTS = TimeStamp,
	    NewObjectTimeStamps = lists:keyreplace(O, 1, ObjectTimeStamps, {O, NewWTS, RTS}),
	    NewOldobj = [{Object, {O, WTS, RTS}} | Oldobj],
	    NewTransactionTimeStamps = lists:keyreplace(
					 Client,
					 1, 
					 TransactionTimeStamps, 
					 {Client, TimeStamp, Deptlist, NewOldobj}),

	     {CurrentTimeStamp, NewTransactionTimeStamps, NewObjectTimeStamps}
    end.

end_transaction(Client, {Clock, TransactionTimeStamps, ObjectTimeStamps}) ->
    NewTransactionTimeStamps = lists:keydelete(Client, 1, TransactionTimeStamps),
    {Clock, NewTransactionTimeStamps, ObjectTimeStamps}.

do_abort(Client, {CurrentTimeStamp, TransactionTimeStamps, ObjectTimeStamps}) ->
    {value, {_, TimeStamp, Deptlist, Oldobj}} = get_transaction(TransactionTimeStamps, Client),
    {NewObjectTimeStamps, RestoreObjList} = do_abort_filter(Oldobj, ObjectTimeStamps, TimeStamp, []),
    %%Gå igenom deplist och uppdatera allas status osv. end_transactino????  do_commit?????
    {{CurrentTimeStamp, TransactionTimeStamps, NewObjectTimeStamps}, RestoreObjList}.

do_abort_filter([{OldObject, {OldO, OldWTS, OldRTS}} | Oldobj], ObjectTimeStamps,  TimeStamp, RestoreObjList)  ->
    {value, {O, WTS, RTS}} = lists:keysearch(OldO, 1, ObjectTimeStamps),
    case WTS =:= TimeStamp of
       true ->
	    NewObjectTimeStamps = lists:keyreplace(O, 1, ObjectTimeStamps, {O, OldWTS, RTS}),
	    do_abort_filter(Oldobj, NewObjectTimeStamps, TimeStamp, [OldObject | RestoreObjList]);
	false ->
	    do_abort_filter(Oldobj, ObjectTimeStamps,  TimeStamp, RestoreObjList)
    end;
do_abort_filter([], ObjectTimeStamps, _, RestoreObjList) ->
    {ObjectTimeStamps, lists:reverse(RestoreObjList)}.

update_dept_status([], TimeStamp) ->  
    

maxx(X,  Y) when X > Y ->
    X;
maxx(_X, Y) ->
    Y.

%% - Low level function to handle lists
add_client(C,T) -> [C|T].

remove_client(_,[]) -> [];
remove_client(C, [C|T]) -> T;
remove_client(C, [H|T]) -> [H|remove_client(C,T)].

all_gone([]) -> true;
all_gone(_) -> false.
