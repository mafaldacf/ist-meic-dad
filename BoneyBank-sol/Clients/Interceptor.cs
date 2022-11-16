using Grpc.Core.Interceptors;
using Grpc.Core;

namespace Clients {

    public class BankClientInterceptor : Interceptor {

        public struct Message {
            public Message(object request, object context, object continuation) {
                Request = request;
                Context = context;
                Continuation = continuation;
            }

            public object Request { get; set; }
            public object Context { get; set; }
            public object Continuation { get; set; }
        }

        private Queue<Message> messageQueue;
        public BankClientInterceptor() : base() {
            messageQueue = new Queue<Message>();
        }

        public override TResponse BlockingUnaryCall<TRequest, TResponse>(TRequest request, ClientInterceptorContext<TRequest, TResponse> context, BlockingUnaryCallContinuation<TRequest, TResponse> continuation) {
            TResponse response;
            int timeout = 10;

            // create new context because original context is readonly
            ClientInterceptorContext<TRequest, TResponse> modifiedContext =
                new ClientInterceptorContext<TRequest, TResponse>(context.Method, context.Host,
                    new CallOptions(context.Options.Headers, context.Options.Deadline,
                        context.Options.CancellationToken, context.Options.WriteOptions,
                        context.Options.PropagationToken, context.Options.Credentials));

            Message message = new Message(request, modifiedContext, continuation);

            // queue message and wait for preceding ones
            lock (messageQueue) {
                messageQueue.Enqueue(message);
                while (!messageQueue.First().Equals(message)) {
                    Monitor.Wait(messageQueue);
                }
            }

            while (true) { // loop until message is successfuly sent; increase sleep time at each iteration
                try {
                    response = base.BlockingUnaryCall(request, modifiedContext, continuation);
                    break;
                } catch (RpcException) {
                    timeout = timeout < 1000 ? timeout * 2 : 1000; // lock at 1000ms
                    Thread.Sleep(timeout); // avoids flooding the server with requests
                }
            }

            // notify waiting threads
            lock (messageQueue) {
                messageQueue.Dequeue();
                Monitor.PulseAll(messageQueue);
            }


            return response;
        }
    }
}
