using ETL.OpinionesWorker.Services;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace ETL.OpinionesWorker
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IServiceProvider _serviceProvider;

        public Worker(ILogger<Worker> logger, IServiceProvider serviceProvider)
        {
            _logger = logger;
            _serviceProvider = serviceProvider;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation(">>> Worker iniciado para CARGA DE DIMENSIONES: {time}", DateTimeOffset.Now);

            try
            {
                using (var scope = _serviceProvider.CreateScope())
                {
                    var dimensionLoader = scope.ServiceProvider.GetRequiredService<DimensionLoader>();
                    await dimensionLoader.CargarTodasLasDimensiones();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al ejecutar el Worker.");
            }

            _logger.LogInformation(">>> Tarea finalizada. El servicio quedará en espera.");
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
    }
}