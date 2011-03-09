using System;

namespace NServiceBus.Unicast.Transport.RabbitMQ
{
	[Serializable]
	public class RabbitMqAddress
	{
		private readonly string broker;
		private readonly string exchange;
		private string queueName;

		public Uri GetBrokerUri()
		{
			return new Uri("rmq://" + broker);
		}

		public string Broker
		{
			get { return broker; }
		}

		public string Exchange
		{
			get { return exchange; }
		}

		public string QueueName
		{
			get { return queueName; }
		}

		public RabbitMqAddress(string broker, string exchange, string queueName)
		{
			this.broker = broker;
			this.exchange = exchange;
			this.queueName = queueName;
		}

		public bool Equals(RabbitMqAddress other)
		{
			if (ReferenceEquals(null, other)) return false;
			if (ReferenceEquals(this, other)) return true;
			return Equals(other.broker, broker) && Equals(other.exchange, exchange) && Equals(other.queueName, queueName);
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != typeof (RabbitMqAddress)) return false;
			return Equals((RabbitMqAddress) obj);
		}

		public override Int32 GetHashCode()
		{
			var hashCode = broker.GetHashCode();
			hashCode = (hashCode*397) ^ exchange.GetHashCode();
			hashCode = (hashCode*397) ^ queueName.GetHashCode();
			return hashCode;
		}

		public override string ToString()
		{
			return "rmq://" + broker + "/" + exchange + "/" + queueName;
		}

		public static RabbitMqAddress FromString(string value)
		{
			var fields = value.Split('/');
			if (fields.Length != 5)
				throw new FormatException("RabbitMQ publication address was badly formatted: " + value);
			return new RabbitMqAddress(fields[2], fields[3], fields[4]);
		}

		public void SetQueueName(string queueName)
		{
			this.queueName = queueName;
		}
	}
}