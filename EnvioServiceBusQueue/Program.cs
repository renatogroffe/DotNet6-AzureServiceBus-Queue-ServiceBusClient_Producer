using Azure.Messaging.ServiceBus;
using Serilog;
using Serilog.Sinks.SystemConsole.Themes;

var logger = new LoggerConfiguration()
    .WriteTo.Console(theme: AnsiConsoleTheme.Literate)
    .CreateLogger();
logger.Information(
    "Testando o envio de mensagens para uma Fila do Azure Service Bus");

if (args.Length < 3)
{
    logger.Error(
        "Informe ao menos 3 parametros: " +
        "no primeiro a string de conexao com o Azure Service Bus, " +
        "no segundo a Fila/Queue a que recebera as mensagens, " +
        "ja no terceito em diante as mensagens a serem " +
        "enviadas a Queue do Azure Service Bus...");
    return;
}

string connectionString = args[0];
string queueName = args[1];

logger.Information($"Queue = {queueName}");

var clientOptions = new ServiceBusClientOptions() { TransportType = ServiceBusTransportType.AmqpWebSockets };
var client = new ServiceBusClient(connectionString, clientOptions);
var sender = client.CreateSender(queueName);
try
{
    using ServiceBusMessageBatch messageBatch = await sender.CreateMessageBatchAsync();
    for (int i = 2; i < args.Length; i++)
    {
        var mensagem = args[i];
        if (!messageBatch.TryAddMessage(new ServiceBusMessage(mensagem)))
            throw new Exception($"The message {i} is too large to fit in the batch.");
        logger.Information($"[Mensagem adicionada] {mensagem}");
    }
    
    await sender.SendMessagesAsync(messageBatch);
    logger.Information($"Concluido o envio de {messageBatch.Count} mensagem(ns)");
}
catch (Exception ex)
{
    logger.Error($"Exceção: {ex.GetType().FullName} | " +
                 $"Mensagem: {ex.Message}");
}
finally
{
    if (client is not null)
    {
        await sender.CloseAsync();
        await sender.DisposeAsync();
        await client.DisposeAsync();

        logger.Information(
            "Conexao com o Azure Service Bus fechada!");
    }       
}