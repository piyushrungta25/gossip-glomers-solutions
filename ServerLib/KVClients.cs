using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Maelstorm.ServerLib;


public abstract class KVClient {
    private Server server;
    private string destination;


    public KVClient(Server server, string destination)
    {
        this.server = server;
        this.destination = destination;
    }

    public async Task<long> Read(string key) {
        return long.Parse(await this.ReadString(key));
    }

    public async Task<string> ReadString(string key) {

        var mess = new Message {
            dest = destination,
            body = new MessageBody {
                type = MsgType.Read,
                key = key,
            }
        };

        Message resp = await server.SendRequestSync(mess);
        if(resp.body.type == MsgType.Error) {
            if(resp.body.code == SeqKVErrorCodes.KeyNotFound)
                throw new KVClientKeyNotFoundException(resp.body.text);
            else
                throw new KVClientException(resp.body.text);
        }

        return resp.body.value!;
        
    }

    
    public async Task Write(string key, long value) {
        await this.WriteString(key, value.ToString());
    }

    public async Task WriteString(string key, string value) {
            var mess = new Message {
                dest = destination,
                body = new MessageBody {
                    type = MsgType.Write,
                    key = key,
                    value = value
                }
            };

            Message resp = await server.SendRequestSync(mess);
            if(resp.body.type != MsgType.WriteOk)
                throw new KVClientException();
    }

    public async Task CAS(string key, long from, long to, bool createIfNotExist) {
            var mess = new Message {
                dest = destination,
                body = new MessageBody {
                    type = MsgType.Cas,
                    key = key,
                    from = from.ToString(),
                    to = to.ToString(),
                    create_if_not_exists = createIfNotExist
                }
            };

            Message resp = await server.SendRequestSync(mess);
            if(resp.body.type == MsgType.Error) {
                if(resp.body.code == SeqKVErrorCodes.KeyNotFound)
                    throw new KVClientKeyNotFoundException(resp.body.text);
                else if(resp.body.code == SeqKVErrorCodes.CASWriteError)
                    throw new KVClientCASWriteException(resp.body.text);
                else
                    throw new KVClientException(resp.body.text);
            }

            if(resp.body.type != MsgType.CasOk)
                throw new KVClientException();
    }

    public async Task<long> ReadOrDefault(string key, long defaultVal)
    {
        try {
            return await this.Read(key);
        } catch(KVClientKeyNotFoundException) {
            return defaultVal;
        }
    }

    public async Task<string> ReadStringOrDefault(string key, long defaultVal)
    {
        try {
            return await this.ReadString(key);
        } catch(KVClientKeyNotFoundException) {
            return defaultVal.ToString();
        }
    }
}


public class SeqKVClient : KVClient {
    public SeqKVClient(Server server): base(server, "seq-kv") {}
}

public class LinKVClient : KVClient {
    public LinKVClient(Server server): base(server, "lin-kv") {}
}


public class KVClientException : Exception
{
    public KVClientException() {}

    public KVClientException(string? message) : base(message) {}
}


public class KVClientKeyNotFoundException : KVClientException
{
    public KVClientKeyNotFoundException(string? message) : base(message) {}
}


public class KVClientCASWriteException : KVClientException
{
    public KVClientCASWriteException(string? message) : base(message) {}
}

public class SeqKVErrorCodes {
    public static int KeyNotFound = 20;
    public static int CASWriteError = 22;
}