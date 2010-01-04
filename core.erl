%% core restmq operations

-module(core).
-include("/opt/lib/erlang/lib/erldis-0.0.11/include/erldis.hrl").
-compile(export_all).

-define (POLICY_BROADCAST,  "1").
-define (POLICY_ROUNDROBIN, "2").


normalize(Key) ->
    Key.

%% this is hackey, the most IFfy use of case/of
queue_set_add(Client, Queue) ->
    Queue_name = Queue ++ ":queue",
    erldis_sets:add_element(list_to_bitstring(Queue_name), Client, <<"ALLQUEUES">>).

queue_unique_id(Client, Queue) ->
    Key = Queue ++ ":UUID",
    case erldis:incr(Client, list_to_bitstring(Key)) of
        true ->
            {Queue, 1};
        Id ->
            {Queue, Id}
    end.

queue_add(Queue, Data) ->
    {ok, Client} = erldis:connect("localhost", 6379),
    {Queue, Id} = queue_unique_id(Client, Queue),
    _ = queue_set_add(Client, Queue),
    Obj_key = lists:append([Queue, [$:], integer_to_list(Id)]),
    Queue_name = Queue ++ ":queue",
    %%Lp = erldis:lpush(Client, list_to_bitstring(Queue_name), list_to_bitstring(Obj_key)),
    Lp = erldis_list:in(list_to_bitstring(Obj_key), list_to_bitstring(Queue_name), Client),
    Sret = erldis:set(Client, Obj_key, list_to_bitstring(Data)),
    erldis_client:stop(Client),
    {Queue, Obj_key, Lp, Sret}.

queue_get(Queue) ->
    queue_get(Queue, false).

queue_get(Queue, GetDel) ->
    {ok, Client} = erldis:connect("localhost", 6379),
    Ret=queue_get(Client, Queue, GetDel),
    erldis_client:stop(Client),
    Ret.

queue_get(Client, Queue, GetDel) ->
    Queue_name = Queue ++ ":queue",
    Nextkey = erldis:lpop(Client, list_to_bitstring(Queue_name)),
    case Nextkey of
        [nil] ->
            {nil, GetDel, nil};
        Nextkey ->
            case GetDel of
                    true ->
                        OldKey = list_to_bitstring(Nextkey),
                        NewKeyStr = Nextkey++":lock",
                        NewKey = list_to_bitstring(NewKeyStr),
                        RRet = erldis:rename(Client, OldKey, NewKey),
                        Val = erldis:get(Client, NewKey),
                        Ret = queue_del(Client, Queue, NewKeyStr),
                        {Val, GetDel, Nextkey, Ret, RRet};
                    _ ->
                        Val = erldis:get(Client, list_to_bitstring(Nextkey)),
                        {Val, GetDel, Nextkey}
                end
    end.

queue_del(Client, Queue, Key) ->
    case erldis:del(Client, list_to_bitstring(Key)) of
        [nil] ->
            {Queue, Key, nil};
        Ret ->
            {Queue, Key, Ret}
    end.

queue_len(Queue) ->
    {ok, Client} = erldis:connect("localhost", 6379),
    Queue_name = Queue ++ ":queue",
    Ret = erldis_list:len(list_to_bitstring(Queue_name), Client),
    erldis_client:stop(Client),
    {Queue, Ret, Queue_name}.

queue_all() ->
    %%smembers
    {ok, Client} = erldis:connect("localhost", 6379),
    QList = erldis_sets:to_list(Client, <<"ALLQUEUES">>),
    erldis_client:stop(Client),
    QList.

queue_getdel(Queue) ->
    queue_get(Queue, true).

queue_policy_set(Queue, Policy) -> %%when Policy == ?POLICY_BROADCAST or Policy == ?POLICY_ROUNDROBIN ->
    {ok, Client} = erldis:connect("localhost", 6379),
    Queue_policy = Queue ++ ":queuepolicy",
    Ret = erldis:set(Client, list_to_bitstring(Queue_policy), list_to_bitstring(Policy)),
    erldis_client:stop(Client),
    {Queue, Policy, Ret}.

queue_policy_get(Queue) ->
    {ok, Client} = erldis:connect("localhost", 6379),
    Queue_policy = Queue ++ ":queuepolicy",
    Ret = erldis:get(Client, list_to_bitstring(Queue_policy)),
    erldis_client:stop(Client),
    {Queue, Ret}.

%% tail family

queue_tail(Queue) ->
    queue_tail(Queue, false, 10).

queue_tail(Queue, Delete) ->
    queue_tail(Queue, Delete, 10).

queue_tail(Queue, Delete, KeyNo) ->
    {ok, Client} = erldis:connect("localhost", 6379),
    Ret = queue_tail_acc(Client, Queue, Delete, [], KeyNo),
    erldis_client:stop(Client),
    {Queue, Ret}.

queue_tail_acc(Client, Queue, Delete, List, N) when N > 0 ->
    {_, _, L} = queue_get(Client, Queue, Delete),
    L2 = List ++ L,
    queue_tail_acc(Client, Queue, Delete, L2, N-1);

queue_tail_acc(_, _, _, List, 0) ->
    List.

%% tail family

queue_count_elements(Queue) ->
    {Queue, 10}.

queue_last_items(Queue) ->
    queue_last_items(Queue, 10).
    
queue_last_items(Queue, Items) ->
    {Queue, Items}.


