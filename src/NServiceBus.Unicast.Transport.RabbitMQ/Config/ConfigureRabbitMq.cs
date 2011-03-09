using System;
using NServiceBus.ObjectBuilder;

namespace NServiceBus.Unicast.Transport.RabbitMQ.Config
{
	public static class ConfigureRabbitMq
	{
		public static ConfigRabbitMqTransport RabbitMqTransport(this Configure config)
		{
			var cfg = new ConfigRabbitMqTransport();
			cfg.Configure(config);
			return cfg;
		}
	}

	public class ConfigRabbitMqTransport : Configure
	{
		private IComponentConfig<RabbitMqTransport> config;

		public void Configure(Configure config)
		{
			this.Builder = config.Builder;
			this.Configurer = config.Configurer;

			this.config = Configurer.ConfigureComponent<RabbitMqTransport>(ComponentCallModelEnum.Singleton);
			var cfg = NServiceBus.Configure.GetConfigSection<RabbitMqTransportConfig>();

			if(cfg != null)
			{
				this.config.ConfigureProperty(t => t.NumberOfWorkerThreads, cfg.NumberOfWorkerThreads);
				this.config.ConfigureProperty(t => t.MaximumNumberOfRetries, cfg.MaxRetries);
				this.config.ConfigureProperty(t => t.ListenAddress, cfg.InputQueue);
				this.config.ConfigureProperty(t => t.PoisonAddress, cfg.ErrorQueue);
				this.config.ConfigureProperty(t => t.TransactionTimeout, TimeSpan.FromMinutes(cfg.TransactionTimeout));
				this.config.ConfigureProperty(t => t.DoNotCreateQueues, cfg.DoNotCreateQueues);
				this.config.ConfigureProperty(t => t.ListenExchangeType, cfg.ListenExchangeType);
				this.config.ConfigureProperty(t => t.SendAcknowledgement, cfg.SendAcknowledgement);
			}
		}

		public ConfigRabbitMqTransport MaximumNumberOfRetries(Int32 value)
		{
			config.ConfigureProperty(t => t.MaximumNumberOfRetries, value);
			return this;
		}

		public ConfigRabbitMqTransport NumberOfWorkerThreads(Int32 value)
		{
			config.ConfigureProperty(t => t.NumberOfWorkerThreads, value);
			return this;
		}

		public ConfigRabbitMqTransport TransactionTimeout(TimeSpan value)
		{
			config.ConfigureProperty(t => t.TransactionTimeout, value);
			return this;
		}

		public ConfigRabbitMqTransport On(string listenAddress, string poisonAddress)
		{
			if (!String.IsNullOrEmpty(listenAddress))
				config.ConfigureProperty(t => t.ListenAddress, listenAddress);
			if (!String.IsNullOrEmpty(poisonAddress))
				config.ConfigureProperty(t => t.PoisonAddress, poisonAddress);
			return this;
		}
	}
}