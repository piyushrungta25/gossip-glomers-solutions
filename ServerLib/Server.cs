using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Timers;


namespace Maelstorm.ServerLib;

public class Server {

    private long MsgId = 0;
    private Dictionary<string, Func<Message, Task<Message?>>> Handlers = new();
    private ConcurrentDictionary<long, TaskCompletionSource<Message>> SyncResponseDictionary = new();
    
    protected string NodeId {get; set;} = "uninitialized";
    protected string[] NodeIds { get; set; }
    private List<System.Timers.Timer> Timers;


    public Server() {
        this.On(MsgType.Init, this.InitHandler);
        this.NodeIds = new string[0];
        this.Timers = new List<System.Timers.Timer>();
    }

    // register handlers
    public void On(string messageType, Func<Message, Task<Message?>> handler) {
        this.Handlers.Add(messageType, handler);
    }

    protected void Log(string msg) {
        Console.Error.WriteLine(msg);
    }

    protected long getNextMessageId()
    {
        return Interlocked.Increment(ref this.MsgId);
    }

    protected string Serialize(Message message)
    {
        return JsonSerializer.Serialize(message, GetSerializationOptions());
    }

    protected Message? Deserialize(string message)
    {
        return JsonSerializer.Deserialize<Message>(message, GetSerializationOptions());
    }

    private JsonSerializerOptions GetSerializationOptions() {
        return new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        };
    }

    private async Task<Message?> InitHandler(Message message) {
        if(message.body.node_id == null)
            this.Log("Found empty node id in init message");
        else
            this.NodeId = message.body.node_id;

        if(message.body.node_ids == null)
            this.Log("Found empty node ids in init message");
        else
            this.NodeIds = message.body.node_ids;

        return new Message {
            body = new MessageBody {
                type = MsgType.Init_Ok
            }
        };   
    }

    // sends a response to a request
    public async Task SendResponse(Message request, Message response)
    {
        response.src = this.NodeId;
        response.dest = request.src;
        response.body.msg_id = response.body.msg_id ?? this.getNextMessageId();
        response.body.in_reply_to = request.body.msg_id;
        await Send(response, "response");
    }

    // sends a new request/message
    public async Task SendMessage(Message message)
    {
        message.src = this.NodeId;
        message.body.msg_id = message.body.msg_id ?? this.getNextMessageId();
        await Send(message, "message");
    }

    // sends a synchronous request
    public async Task<Message> SendRequestSync(Message request, int timeoutMilli = 2000) {
    
        request.src = this.NodeId;

        long msgId = request.body.msg_id ?? this.getNextMessageId();
        request.body.msg_id = msgId;

        var tcs = new TaskCompletionSource<Message>();
        this.SyncResponseDictionary[msgId] = tcs;

        await Send(request, "sync");

        Message response;

        var cts = new CancellationTokenSource();
        var timeoutTask = await Task.WhenAny(tcs.Task, Task.Delay(timeoutMilli, cts.Token));
        if(timeoutTask != tcs.Task) {
            this.Log($"Requesttimed out. Id: {msgId}");
            throw new RPCTimeoutException();
        }
        else {
            cts.Cancel();
            response = await tcs.Task;
            this.Log($"Received RPC respone: {this.Serialize(response)}");
        }
            

        this.SyncResponseDictionary.TryRemove(msgId, out var _);
        return response;
    
    }

    protected async Task<Message?> SendRequestSync(Message request, string dest, int timeoutMilli = 2000) {
        request.dest = dest;
        return await SendRequestSync(request, timeoutMilli);
    }

    protected async Task Send(Message message, string type="out") {
        string resp = this.Serialize(message);
        this.Log($"sending {type}: {resp}");

        Console.WriteLine(resp);
        await Console.Out.FlushAsync();
    }
    
    

    public async Task Start() {

        var stdIn = Console.OpenStandardInput();
        var reader = new StreamReader(stdIn);

        List<Task> tasks = new();

        while(reader.EndOfStream == false) {
            string? messageRaw = await reader.ReadLineAsync();

            if(messageRaw == null) {
                this.Log("No more line to read. Exiting");
                break;
            }

            this.Log($"Received message: {messageRaw}");
            Message? request = this.Deserialize(messageRaw);

            if(request == null || request.body == null || request.body.type == null) {
                this.Log($"Error parsing message: {messageRaw}");
                this.Log("Skipping");
                continue;
            }


            // try providing the message to the right thread, if no one is looking for the response
            // try invoking a handler if available
            long inReplyTo = request.body.in_reply_to ?? -1;
            if(inReplyTo >= 0 && this.SyncResponseDictionary.TryGetValue(inReplyTo, out var tcs))
            {
                this.Log($"received reply for requestid: {inReplyTo} at {DateTimeOffset.Now.ToUnixTimeMilliseconds()}");
                tcs.SetResult(request);
                continue;
            }
            
            if(this.Handlers.TryGetValue(request.body.type, out var handler)) {
                tasks.Add(Task.Run(async () => {
                    var response = await handler(request);
                    if(response != null)
                        await this.SendResponse(request, response);
                }));

                tasks.RemoveAll(i => i.IsCompleted);
            } else {
                this.Log($"No handlers registered for type: {request.body.type}. Dropping message.");
            }
        }

        Task.WaitAll(tasks.ToArray());
    }

    protected void SetupBackGroundJob(ElapsedEventHandler job, int freqInMilli) {
        this.Log($"setting up monitor with freq: {freqInMilli}");
        var timer = new System.Timers.Timer(freqInMilli);
        timer.Elapsed += job;
        timer.AutoReset = true;
        timer.Enabled = true;

        this.Timers.Add(timer);
    }
}
