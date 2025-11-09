using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using System.Threading.Tasks;
using ETL.OpinionesWorker.Models;
using Microsoft.Extensions.Logging;

namespace ETL.OpinionesWorker.Extractors
{
    public class DatabaseExtractor : IExtractor
    {
        private readonly string _connectionString;
        private readonly ILogger<DatabaseExtractor> _logger;

        public DatabaseExtractor(string connectionString, ILogger<DatabaseExtractor> logger)
        {
            _connectionString = connectionString;
            _logger = logger;
        }

        public async Task<List<OpinionData>> ExtractAsync()
        {
            var opinions = new List<OpinionData>();

            try
            {
                _logger.LogInformation("Iniciando extracción desde Base de Datos");

                using (var connection = new SqlConnection(_connectionString))
                {
                    await connection.OpenAsync();

                    var query = @"
                        SELECT 
                            IdReview,
                            IdCliente,
                            IdProducto,
                            Fecha,
                            Comentario,
                            Rating
                        FROM WebReviews";

                    using (var command = new SqlCommand(query, connection))
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            opinions.Add(new OpinionData
                            {
                                IdOpinion = reader["IdReview"].ToString(),
                                IdCliente = reader["IdCliente"].ToString(),
                                IdProducto = reader["IdProducto"].ToString(),
                                Fecha = Convert.ToDateTime(reader["Fecha"]),
                                Comentario = reader["Comentario"].ToString(),
                                Fuente = "Web",
                                Rating = Convert.ToInt32(reader["Rating"]),
                                Clasificacion = null
                            });
                        }
                    }
                }

                _logger.LogInformation($"Extracción completada: {opinions.Count} opiniones");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error en extracción Base de Datos");
            }

            return opinions;
        }

        public string GetSourceName() => "Base de Datos - Web Reviews";
    }
}