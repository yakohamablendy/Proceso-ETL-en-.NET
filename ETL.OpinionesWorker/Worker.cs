using ETL.OpinionesWorker.Extractors;
using ETL.OpinionesWorker.Services;
using Microsoft.Extensions.Logging;

namespace ETL.OpinionesWorker
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IConfiguration _configuration;
        private readonly DataLoader _dataLoader;
        private readonly ILoggerFactory _loggerFactory;

        public Worker(ILogger<Worker> logger, IConfiguration configuration, DataLoader dataLoader, ILoggerFactory loggerFactory)
        {
            _logger = logger;
            _configuration = configuration;
            _dataLoader = dataLoader;
            _loggerFactory = loggerFactory;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    _logger.LogInformation("Iniciando proceso ETL de extraccion en: {time}", DateTimeOffset.Now);

                    await _dataLoader.ClearStagingAsync();

                    var httpClient = new HttpClient();

                    var csvLogger = _loggerFactory.CreateLogger<CsvExtractor>();
                    var dbLogger = _loggerFactory.CreateLogger<DatabaseExtractor>();
                    var apiLogger = _loggerFactory.CreateLogger<ApiExtractor>();

                    var csvExtractor = new CsvExtractor(_configuration["DataSources:CsvFilePath"], csvLogger);
                    var dbExtractor = new DatabaseExtractor(_configuration["DataSources:DatabaseConnectionString"], dbLogger);
                    var apiExtractor = new ApiExtractor(httpClient, _configuration["DataSources:ApiUrl"], apiLogger);

                    _logger.LogInformation("Extrayendo datos de CSV");
                    var csvData = await csvExtractor.ExtractAsync();
                    var csvCount = await _dataLoader.LoadToStagingAsync(csvData, "CSV");
                    _logger.LogInformation("Registros de CSV cargados: {Count}", csvCount);

                    _logger.LogInformation("Extrayendo datos de Base de Datos");
                    var dbData = await dbExtractor.ExtractAsync();
                    var dbCount = await _dataLoader.LoadToStagingAsync(dbData, "Database");
                    _logger.LogInformation("Registros de BD cargados: {Count}", dbCount);

                    _logger.LogInformation("Extrayendo datos de API REST");
                    var apiData = await apiExtractor.ExtractAsync();
                    var apiCount = await _dataLoader.LoadToStagingAsync(apiData, "API");
                    _logger.LogInformation("Registros de API cargados: {Count}", apiCount);

                    var totalCount = await _dataLoader.GetStagingCountAsync();
                    _logger.LogInformation("Total de registros en Staging: {Total}", totalCount);

                    _logger.LogInformation("Proceso ETL completado exitosamente en: {time}", DateTimeOffset.Now);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error durante el proceso ETL");
                }

                await Task.Delay(TimeSpan.FromHours(1), stoppingToken);
            }
        }
    }
}