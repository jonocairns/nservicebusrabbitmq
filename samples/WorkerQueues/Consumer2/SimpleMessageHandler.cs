using System;
using System.Threading;
using MyMessages;
using NServiceBus;

namespace Consumer2
{
	public class SimpleMessageHandler : IHandleMessages<SimpleMessage>
	{
		public void Handle(SimpleMessage message)
		{
			Console.WriteLine("---- Simple Message Received ----");
			Console.WriteLine("---- Payload: ");
			Console.WriteLine(message.Message);
			Console.WriteLine();
			Console.WriteLine("---- Sleeping (3s)");
			Thread.Sleep(3000);
			Console.WriteLine("---- Awake ");
		}
	}
}