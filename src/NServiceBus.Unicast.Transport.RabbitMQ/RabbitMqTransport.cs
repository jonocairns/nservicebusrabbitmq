using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Transactions;
using log4net;
using NServiceBus.Serialization;
using NServiceBus.Unicast.Transport.Msmq;
using NServiceBus.Utils;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace NServiceBus.Unicast.Transport.RabbitMQ
{
	public class RabbitMqTransport : ITransport
	{
		private readonly ConnectionProvider connectionProvider = new ConnectionProvider();
		private readonly ILog log = LogManager.GetLogger(typeof (RabbitMqTransport));
		private readonly List<WorkerThread> workers = new List<WorkerThread>();
		private readonly ReaderWriterLockSlim failuresPerMessageLocker = new ReaderWriterLockSlim();
		private readonly IDictionary<string, Int32> failuresPerMessage = new Dictionary<string, Int32>();
		private RabbitMqAddress listenAddress;
		private RabbitMqAddress poisonAddress;
		//private IHeadersSerializer headersSerializer;
		private Int32 numberOfWorkerThreads;
		private Int32 maximumNumberOfRetries;
		private IsolationLevel isolationLevel;
		private TimeSpan transactionTimeout = TimeSpan.FromMinutes(5);
		private TimeSpan receiveTimeout = TimeSpan.FromSeconds(1);

		private class MessageReceiveProperties
		{
			public string MessageId { get; set; }
			public bool NeedToAbort { get; set; }
		}

		[ThreadStatic] private static MessageReceiveProperties messageContext;

		public bool DoNotCreateQueues { get; set; }

		public void Start()
		{
			CreateQueuesIfNecessary();

			for (var i = 0; i < numberOfWorkerThreads; ++i)
			{
				AddWorkerThread().Start();
			}
		}

		public void ChangeNumberOfWorkerThreads(Int32 targetNumberOfWorkerThreads)
		{
			lock (workers)
			{
				var numberOfThreads = workers.Count;
				if (targetNumberOfWorkerThreads == numberOfThreads) return;
				if (targetNumberOfWorkerThreads < numberOfThreads)
				{
					for (var i = targetNumberOfWorkerThreads; i < numberOfThreads; i++)
					{
						workers[i].Stop();
					}
				}
				else if (targetNumberOfWorkerThreads > numberOfThreads)
				{
					for (var i = numberOfThreads; i < targetNumberOfWorkerThreads; i++)
					{
						AddWorkerThread().Start();
					}
				}
			}
		}

		private void CreateQueuesIfNecessary()
		{
			if (!DoNotCreateQueues)
			{
				var inputQueue = RabbitMqAddress.FromString(ListenAddress);
				var errorQueue = RabbitMqAddress.FromString(PoisonAddress);

				using(var connection = connectionProvider.Open(ProtocolName, inputQueue.Broker, true))
				{
					var channel = connection.Model();

					if (!string.IsNullOrEmpty(inputQueue.Exchange))
						channel.ExchangeDeclare(inputQueue.Exchange, ListenExchangeType);

					if (!string.IsNullOrEmpty(inputQueue.QueueName))
						channel.QueueDeclare(inputQueue.QueueName, false, false, false, null);
					else
					{
						//this is not correct, I would rather queues be autodeleted, having
						//trouble with that
						inputQueue.SetQueueName(
							channel.QueueDeclare("", false, false, false, null));
					}

					if (!string.IsNullOrEmpty(inputQueue.Exchange))
						channel.QueueBind(inputQueue.QueueName, inputQueue.Exchange, string.Empty);

					this.listenAddress = inputQueue;
				}

				using (var connection = connectionProvider.Open(ProtocolName, errorQueue.Broker, true))
				{
					var channel = connection.Model();

					if (!string.IsNullOrEmpty(errorQueue.Exchange))
						channel.ExchangeDeclare(errorQueue.Exchange, "fanout");

					channel.QueueDeclare(errorQueue.QueueName, false, false, false, null);
					
					if (!string.IsNullOrEmpty(errorQueue.Exchange))
						channel.QueueBind(errorQueue.QueueName, errorQueue.Exchange, string.Empty);
				}

			}
		}

		public void Send(TransportMessage transportMessage, string destination)
		{
			var address = RabbitMqAddress.FromString(destination);
			using (var stream = new MemoryStream())
			{
				this.MessageSerializer.Serialize(transportMessage.Body, stream);
				using (var connection = connectionProvider.Open(this.ProtocolName, address.Broker, true))
				{
					var channel = connection.Model();
					var messageId = Guid.NewGuid().ToString();
					var properties = channel.CreateBasicProperties();
					properties.MessageId = messageId;
					if (!String.IsNullOrEmpty(transportMessage.CorrelationId))
					{
						properties.CorrelationId = transportMessage.CorrelationId;
					}

					properties.Timestamp = DateTime.UtcNow.ToAmqpTimestamp();
					properties.ReplyTo = this.ListenAddress;
					properties.SetPersistent(transportMessage.Recoverable);
					var headers = transportMessage.Headers;
					if (headers != null && headers.Count > 0)
					{
						var dictionary = headers
							.ToDictionary<HeaderInfo, string, object>
								(entry => entry.Key, entry => entry.Value);

						properties.Headers = dictionary;
					}
					log.Info("Sending message " + destination + " of " + transportMessage.Body[0].GetType().Name);
					channel.BasicPublish(address.Exchange, address.QueueName, properties, stream.ToArray());
					transportMessage.Id = properties.MessageId;
				}
			}
		}

		public void ReceiveMessageLater(TransportMessage transportMessage)
		{
			if (!String.IsNullOrEmpty(this.ListenAddress))
			{
				Send(transportMessage, this.ListenAddress);
			}
		}

		public Int32 GetNumberOfPendingMessages()
		{
			return 0;
		}

		public void AbortHandlingCurrentMessage()
		{
			if (messageContext != null)
			{
				messageContext.NeedToAbort = true;
			}
		}

		public Int32 NumberOfWorkerThreads
		{
			get
			{
				lock (workers)
				{
					return workers.Count;
				}
			}
			set { numberOfWorkerThreads = value; }
		}

		public Int32 MaximumNumberOfRetries
		{
			get { return maximumNumberOfRetries; }
			set { maximumNumberOfRetries = value; }
		}

		public string Address
		{
			get
			{
				if (listenAddress == null)
					return null;
				return listenAddress.ToString();
			}
		}

		public void Dispose()
		{
			lock (workers)
			{
				foreach (var worker in workers)
				{
					worker.Stop();
				}
			}
		}

		public event EventHandler<TransportMessageReceivedEventArgs> TransportMessageReceived;
		public event EventHandler StartedMessageProcessing;
		public event EventHandler FinishedMessageProcessing;
		public event EventHandler FailedMessageProcessing;

		public string ListenAddress
		{
			get { return listenAddress.ToString(); }
			set { listenAddress = String.IsNullOrEmpty(value) ? null : RabbitMqAddress.FromString(value); }
		}

		public string PoisonAddress
		{
			get { return poisonAddress.ToString(); }
			set { poisonAddress = String.IsNullOrEmpty(value) ? null : RabbitMqAddress.FromString(value); }
		}

		public IMessageSerializer MessageSerializer { get; set; }

		public IsolationLevel IsolationLevel
		{
			get { return isolationLevel; }
			set { isolationLevel = value; }
		}

		public string ProtocolName { get; set; }

		public TimeSpan TransactionTimeout
		{
			get { return transactionTimeout; }
			set { transactionTimeout = value; }
		}

		public TimeSpan ReceiveTimeout
		{
			get { return receiveTimeout; }
			set { receiveTimeout = value; }
		}

		public string ListenExchangeType { get; set; }

		public bool SendAcknowledgement { get; set; }

		private WorkerThread AddWorkerThread()
		{
			lock (workers)
			{
				var newWorker = new WorkerThread(Process);
				workers.Add(newWorker);
				newWorker.Stopped += 
					((sender, e) =>
				        {
				            var worker = sender as WorkerThread;
				            lock (workers)
				            {
				                log.Info("Removing Worker");
				                workers.Remove(worker);
				            }
				        });
				return newWorker;
			}
		}

		private void Process()
		{
			messageContext = new MessageReceiveProperties();
			try
			{
				var wrapper = new TransactionWrapper();
				wrapper.RunInTransaction(() => Receive(messageContext), isolationLevel, transactionTimeout);
				ClearFailuresForMessage(messageContext.MessageId);
			}
			catch (AbortHandlingCurrentMessageException)
			{
			}
			catch (Exception error)
			{
				log.Error(error);
				IncrementFailuresForMessage(messageContext.MessageId);
				OnFailedMessageProcessing(error);
			}
			finally
			{
				messageContext = null;
			}
		}

		private void Receive(MessageReceiveProperties messageContext)
		{
			log.Debug("Receiving from " + listenAddress);

			using (var connection = connectionProvider.Open(this.ProtocolName, listenAddress.Broker, true))
			{
				var channel = connection.Model();
				var consumer = new QueueingBasicConsumer(channel);

				channel.BasicConsume(listenAddress.QueueName, SendAcknowledgement, consumer);

				var delivery = consumer.Receive(receiveTimeout);
				if (delivery != null)
				{
					DeliverMessage(channel, messageContext, delivery);
				}
			}
		}

		private void DeliverMessage(IModel channel, MessageReceiveProperties messageContext, BasicDeliverEventArgs delivery)
		{
			messageContext.MessageId = delivery.BasicProperties.MessageId;
			if (HandledMaximumRetries(messageContext.MessageId))
			{
				MoveToPoison(delivery);
				channel.BasicAck(delivery.DeliveryTag, false);
				return;
			}

			var startedProcessingError = OnStartedMessageProcessing();
			if (startedProcessingError != null)
			{
				throw new MessageHandlingException("Exception occured while starting to process message.", startedProcessingError,
				                                   null, null);
			}

			var m = new TransportMessage();
			try
			{
				using (var stream = new MemoryStream(delivery.Body))
				{
					m.Body = this.MessageSerializer.Deserialize(stream);
				}
			}
			catch (Exception deserializeError)
			{
				log.Error("Could not extract message data.", deserializeError);
				MoveToPoison(delivery);
				OnFinishedMessageProcessing();
				return;
			}
			m.Id = delivery.BasicProperties.MessageId;
			m.CorrelationId = delivery.BasicProperties.CorrelationId;
			m.IdForCorrelation = delivery.BasicProperties.MessageId;
			m.ReturnAddress = delivery.BasicProperties.ReplyTo;
			m.TimeSent = delivery.BasicProperties.Timestamp.ToDateTime();
			m.Headers = m.Headers ?? new List<HeaderInfo>();
			if (delivery.BasicProperties.Headers != null && delivery.BasicProperties.Headers.Count > 0)
			{
				foreach (DictionaryEntry entry in delivery.BasicProperties.Headers)
				{
					var value = entry.Value;
					var valueString = value != null ? value.ToString() : null;

					m.Headers.Add(
						new HeaderInfo
							{
								Key = entry.Key.ToString(),
								Value = valueString
							});
				}
			}

			m.Recoverable = delivery.BasicProperties.DeliveryMode == 2;
			var receivingError = OnTransportMessageReceived(m);
			var finishedProcessingError = OnFinishedMessageProcessing();
			if (messageContext.NeedToAbort)
			{
				throw new AbortHandlingCurrentMessageException();
			}
			if (receivingError != null || finishedProcessingError != null)
			{
				throw new MessageHandlingException("Exception occured while processing message.", null, receivingError,
				                                   finishedProcessingError);
			}
			channel.BasicAck(delivery.DeliveryTag, false);
		}

		private void IncrementFailuresForMessage(string id)
		{
			if (String.IsNullOrEmpty(id)) return;
			failuresPerMessageLocker.EnterWriteLock();
			try
			{
				if (!failuresPerMessage.ContainsKey(id))
				{
					failuresPerMessage[id] = 1;
				}
				else
				{
					failuresPerMessage[id] += 1;
				}
			}
			finally
			{
				failuresPerMessageLocker.ExitWriteLock();
			}
		}

		private void ClearFailuresForMessage(string id)
		{
			if (String.IsNullOrEmpty(id)) return;
			failuresPerMessageLocker.EnterReadLock();
			if (failuresPerMessage.ContainsKey(id))
			{
				failuresPerMessageLocker.ExitReadLock();
				failuresPerMessageLocker.EnterWriteLock();
				failuresPerMessage.Remove(id);
				failuresPerMessageLocker.ExitWriteLock();
			}
			else
			{
				failuresPerMessageLocker.ExitReadLock();
			}
		}

		private bool HandledMaximumRetries(string id)
		{
			if (String.IsNullOrEmpty(id)) return false;
			failuresPerMessageLocker.EnterReadLock();
			if (failuresPerMessage.ContainsKey(id) && (failuresPerMessage[id] == maximumNumberOfRetries))
			{
				failuresPerMessageLocker.ExitReadLock();
				failuresPerMessageLocker.EnterWriteLock();
				failuresPerMessage.Remove(id);
				failuresPerMessageLocker.ExitWriteLock();
				return true;
			}
			failuresPerMessageLocker.ExitReadLock();
			return false;
		}

		private void MoveToPoison(BasicDeliverEventArgs delivery)
		{
			if (poisonAddress == null)
			{
				log.Info("Discarding " + delivery.BasicProperties.MessageId);
				return;
			}
			using (var connection = connectionProvider.Open(this.ProtocolName, listenAddress.Broker, false))
			{
				var channel = connection.Model();
				log.Info("Moving " + delivery.BasicProperties.MessageId + " to " + poisonAddress);
				channel.BasicPublish(poisonAddress.Exchange, poisonAddress.QueueName, delivery.BasicProperties, delivery.Body);
			}
		}

		private Exception OnFailedMessageProcessing(Exception error)
		{
			try
			{
				if (this.FailedMessageProcessing != null)
				{
					this.FailedMessageProcessing(this, new ThreadExceptionEventArgs(error));
				}
			}
			catch (Exception processingError)
			{
				log.Error("Failed raising 'failed message processing' event.", processingError);
				return processingError;
			}
			return null;
		}

		private Exception OnStartedMessageProcessing()
		{
			try
			{
				if (this.StartedMessageProcessing != null)
				{
					this.StartedMessageProcessing(this, null);
				}
			}
			catch (Exception processingError)
			{
				log.Error("Failed raising 'started message processing' event.", processingError);
				return processingError;
			}
			return null;
		}

		private Exception OnFinishedMessageProcessing()
		{
			try
			{
				if (this.FinishedMessageProcessing != null)
				{
					this.FinishedMessageProcessing(this, null);
				}
			}
			catch (Exception processingError)
			{
				log.Error("Failed raising 'finished message processing' event.", processingError);
				return processingError;
			}
			return null;
		}

		private Exception OnTransportMessageReceived(TransportMessage msg)
		{
			try
			{
				if (this.TransportMessageReceived != null)
				{
					this.TransportMessageReceived(this, new TransportMessageReceivedEventArgs(msg));
				}
			}
			catch (Exception processingError)
			{
				log.Error("Failed raising 'transport message received' event.", processingError);
				return processingError;
			}
			return null;
		}
	}

	public static class ConsumerHelpers
	{
		private static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, 0);

		public static BasicDeliverEventArgs Receive(this QueueingBasicConsumer consumer, TimeSpan to)
		{
			object delivery;
			if (!consumer.Queue.Dequeue((Int32) to.TotalMilliseconds, out delivery))
			{
				return null;
			}
			return delivery as BasicDeliverEventArgs;
		}

		public static DateTime ToDateTime(this AmqpTimestamp timestamp)
		{
			return UnixEpoch.AddSeconds(timestamp.UnixTime);
		}

		public static AmqpTimestamp ToAmqpTimestamp(this DateTime dateTime)
		{
			return new AmqpTimestamp((long) (dateTime - UnixEpoch).TotalSeconds);
		}
	}
}