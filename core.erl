%% core restmq operations
%% gleicon@gmail.com
%% http://zenmachine.wordpress.com
%% http://github.com/gleicon/restmq-erl
%%
%%
%% Client = core:redis_connect("localhost", 6379).
%% core:queue_add(Client, "tusca", "some value").
%% core:queue_add(Client, "tusca", "another value").
%% core:queue_len(Client, "tusca").
%% core:queue_last_items(Client, "tusca").
%% core:queue_get(Client, "tusca").
%% core:queue_getdel(Client, "tusca").
%% core:queue_len(Client, "tusca").
%% core:queue_add(Client, "tusca", "some value").
%% core:queue_add(Client, "tusca", "another value").
%% core:queue_tail(Client, "tusca").
%% core:queue_len(Client, "tusca").
%% core:redis_close(Client).
%%


-module(core).
-include("/opt/lib/erlang/lib/erldis-0.0.11/include/erldis.hrl").
-compile(export_all).

-define (POLICY_BROADCAST,  1).
-define (POLICY_ROUNDROBIN, 2).

redis_connect(Host, Port) ->
    {ok, Client} = erldis:connect(Host, Port),
    Client.

redis_close(Client) ->
    erldis_client:stop(Client).


%% TODO: utf-8 normalization
normalize(Key) ->
    Key.

queue_set_add(Client, Queue) ->
    Queue_name = Queue ++ ":queue",
    {"ALLQUEUES", erldis_sets:add_element(list_to_bitstring(Queue_name), Client, <<"ALLQUEUES">>)}.

queue_unique_id(Client, Queue) ->
    Key = Queue ++ ":UUID",
    case erldis:incr(Client, list_to_bitstring(Key)) of
        true ->
            {Queue, 1};
        Id ->
            {Queue, Id}
    end.

queue_add(Client, Queue, Data) ->
    {Queue, Id} = queue_unique_id(Client, Queue),
    _ = queue_set_add(Client, Queue),
    Obj_key = lists:append([Queue, [$:], integer_to_list(Id)]),
    Queue_name = Queue ++ ":queue",
    %%Lp = erldis:lpush(Client, list_to_bitstring(Queue_name), list_to_bitstring(Obj_key)),
    Lp = erldis_list:in(list_to_bitstring(Obj_key), list_to_bitstring(Queue_name), Client),
    Sret = erldis:set(Client, Obj_key, list_to_bitstring(Data)),
    {Queue, Obj_key, Lp, Sret}.

queue_get(Client, Queue) ->
    queue_get(Client, Queue, false).

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
    {Queue, Key, erldis:del(Client, list_to_bitstring(Key))}.

queue_len(Client, Queue) ->
    Queue_name = Queue ++ ":queue",
    {Queue, erldis_list:len(list_to_bitstring(Queue_name), Client), Queue_name}.

queue_all(Client) ->
    erldis_sets:to_list(Client, <<"ALLQUEUES">>).

queue_getdel(Client, Queue) ->
    queue_get(Client, Queue, true).

queue_policy_set(Client, Queue, Policy) when Policy =:= ?POLICY_BROADCAST; Policy == ?POLICY_ROUNDROBIN ->
    Queue_policy = Queue ++ ":queuepolicy",
    {Queue, Policy, erldis:set(Client, list_to_bitstring(Queue_policy), list_to_bitstring([Policy]))}.

queue_policy_get(Client, Queue) ->
    Queue_policy = Queue ++ ":queuepolicy",
    {Queue, erldis:get(Client, list_to_bitstring(Queue_policy))}.

%%
%% tail family
%%
queue_tail(Client, Queue) ->
    queue_tail(Client, Queue, false, 10).

queue_tail(Client, Queue, Delete) ->
    queue_tail(Client, Queue, Delete, 10).

queue_tail(Client, Queue, Delete, KeyNo) ->
    {Queue, queue_tail_acc(Client, Queue, Delete, [], KeyNo)}.

queue_tail_acc(Client, Queue, Delete, List, N) when N > 0 ->
    {_, _, L} = queue_get(Client, Queue, Delete),
    case L of 
        nil -> 
            List;
        L ->
            L2 = List ++ L,
            queue_tail_acc(Client, Queue, Delete, L2, N-1)
    end;


queue_tail_acc(_, _, _, List, 0) ->
    List.

%%
%% count_elements uses KEY command... turns out to be expensive and cause of a lot of broken pipes. It's out for now
%%
queue_count_elements(Client, Queue) ->
    {Queue, 10}.

%%
%% last N items on the queue (non-destructive, but for stats purposes only)
%%
queue_last_items(Client, Queue) ->
    queue_last_items(Client, Queue, 10).
    
queue_last_items(Client, Queue, Items) ->
    Queue_name = Queue ++ ":queue",
    {Queue, erldis:lrange(Client, list_to_bitstring(Queue_name), 0, Items -1)}.

