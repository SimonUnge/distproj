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
	    case transaction_exists(Client, Transactions) of
		true ->
		    case should_sleep(Client, Transactions) of
			true ->
			    NewTransactions = do_sleep(Client, Transactions),
			    server_loop(ClientList, StorePid, NewTransactions);
			false ->
			    NewTransactions = server_confirm(ClientList, StorePid, Transactions),
			    server_loop(ClientList, StorePid, NewTransactions);
		false -> 
		    io:format("Unknown commit; no such transaction"),
		    server_loop(ClientList, StorePid, Transactions)
	    end;
	{action, Client, Act} ->
	    io:format("Received~p from client~p.~n", [Act, Client]),
	    case transaction_exists(Client, Transactions) of
		true ->
		    case valid_action(Act, Client, Transactions) of
			false -> 
			    io:format("Invalid action~n"),
			    {NewTransactions, RestoreList, ClientAbortList} = do_abort(Client, Transactions),
			    StorePid ! {restore, RestoreList, self()},
			    Client ! {abort, self()},
			    server_loop(ClientList,StorePid, NewTransactions);
			true  ->
			    io:format("Valid action~n"),
			    StorePid ! {Act, self()},
			    receive
				{Object, StorePid} ->
				    NewTransactions = update_transaction(Act, Client, Object, Transactions),
				    server_loop(ClientList,StorePid, NewTransactions)
			    end;
			skip  ->
			    io:format("Skip write thanks to Thomas~n"),
			    server_loop(ClientList, StorePid, Transactions)
		    end;
		false ->
		    io:format("No such transacion~n"),
		    server_loop(ClientList, StorePid, Transactions)
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
	    store_loop(ServerPid, Database);
	{restore, RestoreList, ServerPid} ->
	    NewDatabase = restore(RestoreList, Database),
	    store_loop(ServerPid, NewDatabase)
    end.

restore([{O, V} | Rest ], Database) ->
    NewDatabase = lists:keyreplace(O, 1, Database, {O, V}),
    restore(Rest, NewDatabase);
restore([], NewDatabase) ->
    NewDatabase.

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
	{write, _, _} when R =< Timestamp -> %Valid Write
	    case W =< Timestamp of %Check Thomas write rule
		true ->
		    true;
		false  -> %Skip due to Thomas write rule
		    skip
	    end;
	{write, _, _} when R > Timestamp -> %Invalid Write
	    false
    end.

get_object_timestamp(ObjectTimeStamps, {write, O, _}) ->
    lists:keysearch(O, 1, ObjectTimeStamps);
get_object_timestamp(ObjectTimeStamps, {read, O}) ->
    lists:keysearch(O, 1, ObjectTimeStamps).

get_transaction(TransactionTimeStamp, Client) ->
    lists:keysearch(Client, 1, TransactionTimeStamp).

transaction_exists(Client, {_CurrentTimeStamp, TransactionTimeStamps, _ObjectTimeStamps}) ->
    lists:keymember(Client, 1 , TransactionTimeStamps).

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
    {value, {_, TimeStamp, _Deptlist, Oldobj}} = get_transaction(TransactionTimeStamps, Client),
    {NewObjectTimeStamps, RestoreObjList} = do_abort_filter(Oldobj, ObjectTimeStamps, TimeStamp, []),
    NewTransactionTimeStamps = update_dept_status(TransactionTimeStamps, TimeStamp),
    NewTransactions = end_transaction(Client, {CurrentTimeStamp, NewTransactionTimeStamps, NewObjectTimeStamps}),
    {NewTransactions, RestoreObjList}.

do_abort_filter([{OldObject, {OldO, OldWTS, _OldRTS}} | Oldobj], ObjectTimeStamps,  TimeStamp, RestoreObjList)  ->
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


update_dept_status([{ClientPid, TransacitonTimeStamp, {Status, DeptList}, OldObjects} | Rest], TimeStamp) -> 
    UpdatedStatus = update_status(Status, DeptList, TimeStamp),
    [{ClientPid, TransacitonTimeStamp, {UpdatedStatus, DeptList}, OldObjects} | update_dept_status(Rest, TimeStamp)];
update_dept_status([], _) -> 
    [].

%%Change status to abort if dependencies exists...
update_status(Status, DeptList, TimeStamp) ->
    case lists:keymember(TimeStamp, 2, DeptList) of
	true ->
	    abort;
	false ->
	    Status
    end.

can_commit(Client, {_, TransactionTimeStamps, _}) ->
    {value, {_, _, Deptlist, _}} = get_transaction(TransactionTimeStamps, Client),
    {Status, _} = Deptlist,
    %%Status =:= ok.
    case Status of
	abort ->
	    false;
	_ ->
	    true
    end.

do_sleep(Client, {CurrentTimeStamp,  TransactionTimeStamps, ObjectTimeStamps}) ->
    {value, {Client, TS, {Status, Deptlist}, OldObjlist}} = get_transaction(TransactionTimeStamps, Client),
    %%NewStatus = need_sleep(Status, Deptlist, lists:keydelete(Client, 1, TransactionTimeStamps)),
    NewTransactionTimeStamps = lists:keyreplace(Client, 1, TransactionTimeStamps, {Client, TS, {sleep, Deptlist}, OldObjlist}),
    {CurrentTimeStamp,  NewTransactionTimeStamps, ObjectTimeStamps}.

wake( Transaction = {CurrentTimeStamp,  Rest, ObjectTimeStamps}, StorePid)  ->
    wake1(Transaction = {CurrentTimeStamp,  [{Client, TS, {sleep, Deptlist}, OldObjlist} | Rest], ObjectTimeStamps}, StorePid, Rest)
    case should_sleep(Client, Transaction ) of
	true ->
	    ;
	false ->
	    NewTransaction =  server_confirm(Client, Transaction, StorePid),
	    
    end

should_sleep(Client, {CurrentTimeStamp,  TransactionTimeStamps, ObjectTimeStamps}) ->
    {value, {Client, TS, {Status, Deptlist}, OldObjlist}} = get_transaction(TransactionTimeStamps, Client),
    need_sleep(Deptlist, lists:keydelete(Client, 1, TransactionTimeStamps)).

need_sleep([{_,WTS,_} | Rest], TransactionTimeStamps) ->
    case lists:keymember(WTS, 2, TransactionTimeStamps) of
	true ->
	    true;
	false  ->
	    need_sleep(Rest, TransactionTimeStamps)
    end;
need_sleep([], _) ->
    false.
    
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



server_confirm(Client, Transactions, StorePid) ->
    case can_commit(Client, Transactions) of
	true ->
	    io:format("Committed ~p~n",[Transactions]),
	    NewTransactions = end_transaction(Client, Transactions),
	    NewerTransaction = wake(NewTransactions, StorePid),
	    Client ! {committed, self()},
	    NewTransactions;
	false ->
	    server_abort(Client, Transactions, StorePid)
    end.

server_abort(Client, Transactions, StorePid) ->
    io:format("Transaction aborted!"),
    {NewTransactions, RestoreList} = do_abort(Client, Transactions),
    StorePid ! {restore, RestoreList, self()},
    Client ! {abort, self()},
    NewTransactions.

check_abort([{ClientPid,_,{abort,_},_} | Rest])
