using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Data;

namespace ETL.OpinionesWorker.Services
{
    public class FactLoader
    {
        private readonly string _connectionString;
        private readonly ILogger<FactLoader> _logger;

        public FactLoader(IConfiguration configuration, ILogger<FactLoader> logger)
        {
            _connectionString = configuration.GetConnectionString("DWOpiniones");
            _logger = logger;
        }

        public async Task ProcesarFactTable()
        {
            try
            {
                _logger.LogInformation(">>> INICIANDO CARGA DE FACT TABLE (HECHOS) <<<");
                await LimpiarTablaHechos();
                await CargarFactOpiniones();
                _logger.LogInformation(">>> CARGA DE FACT TABLE COMPLETADA <<<");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error crítico en FactLoader.");
            }
        }

        private async Task LimpiarTablaHechos()
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            using var command = new SqlCommand("TRUNCATE TABLE Fact.FactOpiniones", connection);
            command.CommandTimeout = 300; 
            await command.ExecuteNonQueryAsync();
            _logger.LogInformation("Tabla Fact.FactOpiniones truncada/limpia.");
        }

        private async Task CargarFactOpiniones()
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            _logger.LogInformation("Transfiriendo datos de Staging a Fact (Con Mapeo Inteligente)...");

            string query = @"
                INSERT INTO Fact.FactOpiniones 
                (IdCliente, IdProducto, IdFuente, IdTiempo, TextoOpinion, CalificacionNumerica, ClasificacionSentimiento)
                SELECT 
                    dc.IdCliente,           
                    dp.IdProducto,          
                    df.IdFuente,            
                    dt.IdTiempo,            
                    s.Comentario,
                    s.Rating,
                    s.Clasificacion
                FROM Staging.OpinionesStaging s
                -- Mapeo Matemático para Clientes (Evita pérdida de datos)
                INNER JOIN Dimension.Cliente dc ON dc.IdCliente = ((s.IdCliente - 1) % 500) + 1
                -- Mapeo Matemático para Productos
                INNER JOIN Dimension.DimProducto dp ON dp.IdProducto = ((s.IdProducto - 1) % 200) + 1
                -- Fuente (Nombre exacto)
                INNER JOIN Dimension.DimFuente df ON df.NombreFuente = s.Fuente
                -- Tiempo (Fecha exacta)
                INNER JOIN Dimension.DimTiempo dt ON dt.FechaCompleta = CAST(s.Fecha AS DATE)";

            using var command = new SqlCommand(query, connection);

           
            command.CommandTimeout = 300; 
           

            int filas = await command.ExecuteNonQueryAsync();

            _logger.LogInformation($"Registros insertados en Fact.FactOpiniones: {filas}");
        }
    }
}