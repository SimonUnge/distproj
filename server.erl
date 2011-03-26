%%% @author Jon Borglund and Simon Unge
%%% Created : 26 Mar 2011 by Simon Unge <simonunge@nl119-202-125.student.uu.se>

%% We choosed time stamp based concurrency controll, following the algorithm linked to (wikipedia).

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
%% Added a few data structures to make it work.
initialize() ->
    process_flag(trap_exit, true),
    Initialvals = [{a,0},{b,0},{c,0},{d,0}], %% All variables are set to 0
    ObjectTimeStamps = [{a,0,0}, {b,0,0}, {c,0,0},{d,0,0}],  % {object, writetimestamp, readtimestamp}
    TransactionTimeStamps = [],    %[{clientpid,transactionstimestamp,{ok, [dependencies]},[OldObjects]},...]
    CurrentTimeStamp = 0,
    Transactions = {CurrentTimeStamp, TransactionTimeStamps, ObjectTimeStamps},
    ServerPid = self(),
    StorePid = spawn_link(fun() -> store_loop(ServerPid,Initialvals) end),
    server_loop([],StorePid, Transactions, []). %server_loop(ClientList, StorePid, Transactions, Seqlist)
%%%%%%%%%%%%%%%%%%%%%%% STARTING SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%% ACTIVE SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% - The server maintains a list of all connected clients and a store holding
%% the values of the global variable a, b, c and d
server_loop(ClientList,StorePid, Transactions, Seqlist) ->
    receive
    {login, MM, Client} ->
        MM ! {ok, self()},
        io:format("New client has joined the server:~p.~n", [Client]),
        StorePid ! {print, self()},
        server_loop(add_client(Client,ClientList),StorePid, Transactions, Seqlist);
    {close, Client} ->
        io:format("Client~p has left the server.~n", [Client]),
        StorePid ! {print, self()},
        server_loop(remove_client(Client,ClientList),StorePid, Transactions, Seqlist);
    {request, Client} ->
	 %% Starts a new transaction, inits the sequence list.
        NewTransactions = start_transaction(Client, Transactions),
        Client ! {proceed, self()},
	NewSeqlist = init_seq(Client, Seqlist),
        server_loop(ClientList,StorePid, NewTransactions, NewSeqlist);
    {confirm, Client, Seq} ->
        case transaction_exists(Client, Transactions) of
        true ->
		case is_next_in_seq(Client, Seqlist, Seq) of 
		    true ->
			case should_sleep(Client, Transactions) of
			    true ->
				io:format("Client~p sleeping.~n", [Client]),
				NewTransactions = do_sleep(Client, Transactions),
				server_loop(ClientList, StorePid, NewTransactions, Seqlist);
			    false ->
				NewTransactions = server_confirm(Client, Transactions, StorePid),
				NewSeqlist = end_seq(Client, Seqlist),
				server_loop(ClientList, StorePid, NewTransactions, NewSeqlist)
			end;
		    false ->
			Client ! {resend, get_seq_number(Client, Seqlist), self()},
			server_loop(ClientList, StorePid, Transactions, Seqlist)
		end;
        false ->
            io:format("Unknown commit; no such transaction"),
            server_loop(ClientList, StorePid, Transactions, Seqlist)
        end;
    {action, Client, Act, Seq} ->
        io:format("Received~p from client~p.~n", [Act, Client]),
        case transaction_exists(Client, Transactions) of
        true ->
            case is_next_in_seq(Client, Seqlist, Seq) of
		true ->
		    NewSeqlist = increase_seq_number(Client, Seqlist),
		    case valid_action(Act, Client, Transactions) of
			false ->
			    io:format("Client~p send invalid action.~n", [Client]),
			    NewTransactions = server_abort(Client, Transactions, StorePid),
			    server_loop(ClientList,StorePid, NewTransactions, NewSeqlist);
			true  ->
			    io:format("Client~p sent valid action.~n", [Client]),
			    StorePid ! {Act, self()},
			    receive
				{Object, StorePid} ->
				    NewTransactions = update_transaction(Act, Client, Object, Transactions),
				    server_loop(ClientList,StorePid, NewTransactions, NewSeqlist)
			    end;
			skip  ->
			    io:format("Skip write thanks to Thomas~n"),
			    server_loop(ClientList, StorePid, Transactions, NewSeqlist)
		    end;
		false ->
		    io:format("Bad seq number ~p~n",[Seq]),
		    server_loop(ClientList, StorePid, Transactions, Seqlist)
	    end;	    
        false ->
            io:format("No such transacion~n"),
            server_loop(ClientList, StorePid, Transactions, Seqlist)
        end

    after 50000 ->
    case all_gone(ClientList) of
        true -> exit(normal);
        false -> server_loop(ClientList,StorePid, Transactions, Seqlist)
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
    io:format("Store: Restored Object ~p to  value ~p ~n", [O, V]),
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

set_client_status(Client, Status, {CurrentTimeStamp, TransactionTimeStamps, ObjectTimeStamps}) ->
         {value, {Client, TimeStamp, {_, Deptlist}, Oldobj}} = get_transaction(TransactionTimeStamps, Client),
          NewTransactionTimeStamps = lists:keyreplace(
                      Client,
                      1,
                      TransactionTimeStamps,
                      {Client, TimeStamp, {Status,Deptlist}, Oldobj}),
         {CurrentTimeStamp, NewTransactionTimeStamps, ObjectTimeStamps}.

get_object_timestamp(ObjectTimeStamps, {write, O, _}) ->
    lists:keysearch(O, 1, ObjectTimeStamps);
get_object_timestamp(ObjectTimeStamps, {read, O}) ->
    lists:keysearch(O, 1, ObjectTimeStamps).

get_transaction(TransactionTimeStamp, Client) ->
    lists:keysearch(Client, 1, TransactionTimeStamp).

transaction_exists(Client, {_CurrentTimeStamp, TransactionTimeStamps, _ObjectTimeStamps}) ->
    lists:keymember(Client, 1 , TransactionTimeStamps).

%% This poorly named function does crazy much.
%% If read: update the set of dependencies DEP(Ti).add(WTS(Oj)) and set RTS(Oj) = max(RTS(Oj),TS(Ti))
%% If write: store the previous values, OLD(Ti).add(Oj,WTS(Oj)), set WTS(Oj) = TS(Ti), and update the value of Oj.
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

%% Updates the dependencies in transaction to a abort status.
init_abort(Client, {CurrentTimeStamp, TransactionTimeStamps, ObjectTimeStamps}) ->
    {value, {_, TimeStamp, _Deptlist, _Oldobj}} = get_transaction(TransactionTimeStamps, Client),
    NewTransactionTimeStamps = update_dept_status(TransactionTimeStamps, TimeStamp),
    {CurrentTimeStamp, NewTransactionTimeStamps, ObjectTimeStamps}.

%% Terminates the transaction and gets a restore list for restore.
do_abort(Client, {CurrentTimeStamp, TransactionTimeStamps, ObjectTimeStamps}) ->
    {value, {_, TimeStamp, _Deptlist, Oldobj}} = get_transaction(TransactionTimeStamps, Client),
    {NewObjectTimeStamps, RestoreObjList} = do_abort_filter(Oldobj, ObjectTimeStamps, TimeStamp, []),
    NewTransactions = end_transaction(Client, {CurrentTimeStamp, TransactionTimeStamps, NewObjectTimeStamps}),
    {NewTransactions, lists:reverse(RestoreObjList)}.

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
    {ObjectTimeStamps, RestoreObjList}.

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
    case Status of
    abort ->
        false;
    _ ->
        true
    end.

do_sleep(Client, {CurrentTimeStamp,  TransactionTimeStamps, ObjectTimeStamps}) ->
    {value, {Client, TS, {_Status, Deptlist}, OldObjlist}} = get_transaction(TransactionTimeStamps, Client),
    NewTransactionTimeStamps = lists:keyreplace(Client, 1, TransactionTimeStamps, {Client, TS, {sleep, Deptlist}, OldObjlist}),
    {CurrentTimeStamp,  NewTransactionTimeStamps, ObjectTimeStamps}.

%% Tries to wake up sleeping transactions
wake( Transaction = {_CurrentTimeStamp,  Rest, _ObjectTimeStamps}, StorePid)  ->
    wake(Transaction, StorePid, Rest).

wake(Transaction, StorePid, [{Client, _TS, {sleep, _Deptlist}, _OldObjlist} | Rest]) ->
    case should_sleep(Client, Transaction ) of
    true ->
        wake(Transaction, StorePid, Rest);
    false ->
        NewTransaction = {_TS, NewTransactionTimeStamps, _ObjectTimeStamps} = server_confirm(Client, Transaction, StorePid),
        wake(NewTransaction, StorePid, NewTransactionTimeStamps)
    end;
wake(Transaction,StorePid, [_ | Rest]) ->
    wake(Transaction, StorePid, Rest);
wake(Transaction, _, []) ->
    Transaction.

%% Tries to abort transactions which status is abort.
call_abort (Transaction = {_CurrentTimeStamp,  Rest, _ObjectTimeStamps}, StorePid)  ->
    call_abort(Transaction, StorePid, Rest).

call_abort(Transaction = {_,TransList,_}, StorePid, TransList = [{Client, TS, {abort, _Deptlist}, _OldObjlist} | Rest]) ->
    case can_abort(TS, TransList) of
    true ->
        NewTransaction = server_abort(Client, Transaction, StorePid),
        call_abort(NewTransaction, StorePid);
    false ->
        call_abort(Transaction, StorePid, Rest)
    end;
call_abort(Transaction, StorePid, [_ | Rest]) ->
    call_abort(Transaction, StorePid, Rest);
call_abort(Transaction, _, []) ->
    Transaction.

can_abort_client(Client, {_,TransList,_}) ->
    {value, {Client, TS, {_Status, _Deptlist}, _OldObjlist}} = get_transaction(TransList, Client),
    can_abort(TS, TransList).

%%  Checks dependencies    
can_abort(TS, [{_ClientPid, _TStamp, {_Status , Deptlist}, _Oldlist} | TransList])->
    can_abort1(TS, Deptlist) andalso can_abort(TS, TransList);
can_abort(_, []) ->
    true.

can_abort1(TS, [{_,WTS,_}|Rest]) ->
    WTS =/= TS andalso can_abort1(TS, Rest);
can_abort1(_, []) ->
    true.

should_sleep(Client, {_CurrentTimeStamp,  TransactionTimeStamps, _ObjectTimeStamps}) ->
    {value, {Client, _TS, {_Status, Deptlist}, _OldObjlist}} = get_transaction(TransactionTimeStamps, Client),
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

%% Server checks if it is okey to confirm and commits if ok, else sleep or abort...
server_confirm(Client, Transactions, StorePid) ->
    io:format("Serverconfirm transactions~p~n", [Transactions]),
    case can_commit(Client, Transactions) of
    true ->
        io:format("Committed ~p~n",[Transactions]),
        NewTransactions = end_transaction(Client, Transactions),
        NewerTransactions = wake(NewTransactions, StorePid),
        Client ! {committed, self()},
        NewerTransactions;
    false ->
        io:format("Inside server confirm false"),
        server_abort(Client, Transactions, StorePid)
    end.

%% Server checks if it is okey to abort, and also tries to abort dependencies.
server_abort(Client, ATransactions, StorePid) ->
    io:format("Transaction aborted!"),
    Transactions = init_abort(Client, ATransactions),
    case can_abort_client(Client, Transactions) of
        true ->
            {NewTransactions, RestoreList} = do_abort(Client, Transactions),
            StorePid ! {restore, RestoreList, self()},
            Client ! {abort, self()},
            NewerTransactions = call_abort(NewTransactions, StorePid),
            EvenNewerTransactions = wake(NewerTransactions, StorePid),
            EvenNewerTransactions;
        false ->
            NewTransactions = set_client_status(Client, abort, Transactions),
            NewerTransactions = call_abort(NewTransactions, StorePid),
            EvenNewerTransactions = wake(NewerTransactions, StorePid),
            EvenNewerTransactions
    end.

%% SEQUENCE HANDLING for handling lost messages.

init_seq(Client, Seqlist) ->
    [{Client, 0} | end_seq(Client, Seqlist)].

end_seq(Client, Seqlist) ->
    lists:keydelete(Client, 1, Seqlist).

get_seq_number(Client, Seqlist) ->
    {value, {Client, Seqnum}} = lists:keysearch(Client, 1, Seqlist),
    Seqnum.

set_seq_number(Client, Seqlist, Seqnumber) ->
    lists:keyreplace(Client, 1, Seqlist, {Client, Seqnumber}).

is_next_in_seq(Client, Seqlist, Seqnumber) ->
    get_seq_number(Client, Seqlist) =:= Seqnumber.

increase_seq_number(Client, Seqlist) ->
    NextSeqnumber = get_seq_number(Client, Seqlist) + 1,
    set_seq_number(Client, Seqlist, NextSeqnumber).
