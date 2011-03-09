using System.Configuration;

namespace NServiceBus.Unicast.Transport.RabbitMQ.Config
{
	public class RabbitMqTransportConfig : ConfigurationSection
	{
		[ConfigurationProperty("ErrorQueue", IsRequired = true)]
		public string ErrorQueue
		{
			get { return base["ErrorQueue"] as string; }
			set { base["ErrorQueue"] = value; }
		}

		[ConfigurationProperty("InputQueue", IsRequired = true)]
		public string InputQueue
		{
			get { return base["InputQueue"] as string; }
			set { base["InputQueue"] = value; }
		}

		[ConfigurationProperty("MaxRetries", IsRequired = true)]
		public int MaxRetries
		{
			get { return (int) base["MaxRetries"]; }
			set { base["MaxRetries"] = value; }
		}

		[ConfigurationProperty("NumberOfWorkerThreads", IsRequired = true)]
		public int NumberOfWorkerThreads
		{
			get { return (int) base["NumberOfWorkerThreads"]; }
			set { base["NumberOfWorkerThreads"] = value; }
		}

		[ConfigurationProperty("TransactionTimeout", IsRequired = false, DefaultValue = 5)]
		public int TransactionTimeout
		{
			get { return (int)base["TransactionTimeout"]; }
			set { base["TransactionTimeout"] = value; }
		}

		[ConfigurationProperty("DoNotCreateQueues", IsRequired = false, DefaultValue = false)]
		public bool DoNotCreateQueues
		{
			get { return (bool) base["DoNotCreateQueues"]; }
			set { base["DoNotCreateQueues"] = value; }
		}

		[ConfigurationProperty("ListenExchangeType", IsRequired = false, DefaultValue = "direct")]
		public string ListenExchangeType
		{
			get { return base["ListenExchangeType"] as string; }
			set { base["ListenExchangeType"] = value; }
		}

		[ConfigurationProperty("SendAcknowledgement", IsRequired = false, DefaultValue = true)]
		public bool SendAcknowledgement
		{
			get { return (bool) base["SendAcknowledgement"]; }
			set { base["SendAcknowledgement"] = value; }
		}
	}
}