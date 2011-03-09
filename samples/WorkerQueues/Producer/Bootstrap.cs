using System;
using MyMessages;
using NServiceBus;

namespace Producer
{
	public class Bootstrap : IWantToRunAtStartup
	{
		public IBus Bus { get; set; }

		public void Run()
		{
			Console.WriteLine("Press 'Enter' to send a message. To exit, Ctrl + C");

			while (Console.ReadLine() != null)
			{
				var payload = "Hello World";
				Console.WriteLine("Sending Message: {0}", payload);
				Bus.Send<SimpleMessage>( m => { m.Message = payload; });
			}
		}

		public void Stop()
		{
		}
	}
}