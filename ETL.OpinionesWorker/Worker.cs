using ETL.OpinionesWorker.Extractors;
using ETL.OpinionesWorker.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;

namespace ETL.OpinionesWorker
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IConfiguration _configuration;
        private readonly IServiceProvider _serviceProvider;
        private readonly DataLoader _dataLoader;
        private readonly ILoggerFactory _loggerFactory;

        public Worker(ILogger<Worker> logger, IConfiguration configuration, IServiceProvider serviceProvider, DataLoader dataLoader, ILoggerFactory loggerFactory)
        {
            _logger = logger;
            _configuration = configuration;
            _serviceProvider = serviceProvider;
            _dataLoader = dataLoader;
            _loggerFactory = loggerFactory;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation(">>> INICIANDO PROCESO MAESTRO <<<");

            try
            {
                using (var scope = _serviceProvider.CreateScope())
                {
                    _logger.LogInformation("--- FASE 0: Reparando Dimensiones ---");
                    var dimLoader = scope.ServiceProvider.GetRequiredService<DimensionLoader>();
                    await dimLoader.CargarTodasLasDimensiones();

                    _logger.LogInformation("--- FASE 1: Carga a Staging ---");
                    await _dataLoader.ClearStagingAsync();

                    string csvPath = _configuration["DataSources:CsvFilePath"];
           
                    var csvExtractor = new CsvExtractor(csvPath, _loggerFactory.CreateLogger<CsvExtractor>());

                    var datosCsv = await csvExtractor.ExtractAsync();

                    await _dataLoader.LoadToStagingAsync(datosCsv, "CSV Real");
                    
                    _logger.LogInformation("--- FASE 2: Carga de Facts ---");
                    var factLoader = scope.ServiceProvider.GetRequiredService<FactLoader>();
                    await factLoader.ProcesarFactTable();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error crítico en el Worker.");
            }

            _logger.LogInformation(">>> PROCESO FINALIZADO. REVISA LA BASE DE DATOS <<<");
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
    }
}