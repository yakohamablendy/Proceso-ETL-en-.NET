using Microsoft.Data.SqlClient;
using ETL.OpinionesWorker.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Data.SqlTypes;

namespace ETL.OpinionesWorker.Services
{
    public class DataLoader
    {
        private readonly string _connectionString;
        private readonly ILogger<DataLoader> _logger;

        public DataLoader(IConfiguration configuration, ILogger<DataLoader> logger)
        {

            _connectionString = configuration.GetConnectionString("DWOpiniones")
                                ?? configuration["StagingConnectionString"]
                                ?? throw new ArgumentNullException("Connection string no configurada");
            _logger = logger;
        }

        public async Task<int> LoadToStagingAsync(List<OpinionData> opiniones, string sourceName)
        {
            if (opiniones == null || !opiniones.Any())
            {
                _logger.LogWarning("No hay datos para cargar desde {SourceName}", sourceName);
                return 0;
            }

            int insertedCount = 0;

  
            var query = @"
                INSERT INTO Staging.OpinionesStaging 
                    (IdCliente, IdProducto, Fecha, Comentario, Fuente, Rating, Clasificacion, FechaCarga) 
                VALUES 
                    (@IdCliente, @IdProducto, @Fecha, @Comentario, @Fuente, @Rating, @Clasificacion, @FechaCarga)";

            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    await connection.OpenAsync();
                    foreach (var opinion in opiniones)
                    {
                        using (var command = new SqlCommand(query, connection))
                        {
                        
                            command.Parameters.AddWithValue("@IdCliente", (object)opinion.IdCliente ?? DBNull.Value);
                            command.Parameters.AddWithValue("@IdProducto", (object)opinion.IdProducto ?? DBNull.Value);

                          
                            var fechaSafe = (opinion.Fecha < SqlDateTime.MinValue.Value) ? SqlDateTime.MinValue.Value : opinion.Fecha;
                            command.Parameters.AddWithValue("@Fecha", fechaSafe);

                            command.Parameters.AddWithValue("@Comentario", (object)opinion.Comentario ?? DBNull.Value);
                            command.Parameters.AddWithValue("@Fuente", (object)opinion.Fuente ?? sourceName);
                            command.Parameters.AddWithValue("@Rating", (object)opinion.Rating ?? DBNull.Value);
                            command.Parameters.AddWithValue("@Clasificacion", (object)opinion.Clasificacion ?? "Neutro");
                            command.Parameters.AddWithValue("@FechaCarga", DateTime.Now);

                            await command.ExecuteNonQueryAsync();
                            insertedCount++;
                        }
                    }
                }
                _logger.LogInformation("{Count} registros insertados exitosamente desde {SourceName}", insertedCount, sourceName);
                return insertedCount;
            }
            catch (SqlException ex)
            {
                _logger.LogError(ex, "Error SQL al cargar datos desde {SourceName} a Staging", sourceName);
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al cargar datos desde {SourceName} a Staging", sourceName);
                throw;
            }
        }

        public async Task<int> GetStagingCountAsync()
        {
            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    await connection.OpenAsync();
                    var query = "SELECT COUNT(*) FROM Staging.OpinionesStaging";
                    using (var command = new SqlCommand(query, connection))
                    {
                        var count = (int?)await command.ExecuteScalarAsync() ?? 0;
                        _logger.LogInformation("Total de registros en Staging: {Count}", count);
                        return count;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al obtener conteo de Staging");
                return 0;
            }
        }

        public async Task ClearStagingAsync()
        {
            try
            {
                _logger.LogInformation("Limpiando tabla Staging");
                using (var connection = new SqlConnection(_connectionString))
                {
                    await connection.OpenAsync();
                    var query = "TRUNCATE TABLE Staging.OpinionesStaging";
                    using (var command = new SqlCommand(query, connection))
                    {
                        await command.ExecuteNonQueryAsync();
                    }
                }
                _logger.LogInformation("Tabla Staging limpiada exitosamente");
            }
            catch (SqlException ex)
            {
                _logger.LogError(ex, "Error SQL al limpiar tabla Staging");
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al limpiar tabla Staging");
                throw;
            }
        }
    }
}