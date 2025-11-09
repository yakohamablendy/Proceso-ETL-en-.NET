using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Json; // Usaremos esto para que sea más fácil y robusto
using System.Threading.Tasks;
using ETL.OpinionesWorker.Models;
using Microsoft.Extensions.Logging;

namespace ETL.OpinionesWorker.Extractors
{
    public class ApiExtractor : IExtractor
    {
        private readonly HttpClient _httpClient;
        private readonly string _apiUrl;
        private readonly ILogger<ApiExtractor> _logger;

        public ApiExtractor(HttpClient httpClient, string apiUrl, ILogger<ApiExtractor> logger)
        {
            _httpClient = httpClient;
            _apiUrl = apiUrl;
            _logger = logger;
        }

        public async Task<List<OpinionData>> ExtractAsync()
        {
            var opinions = new List<OpinionData>();
            try
            {
                _logger.LogInformation("Iniciando extracción desde API: {ApiUrl}", _apiUrl);

                // Esta línea es más moderna y maneja la deserialización automáticamente
                var records = await _httpClient.GetFromJsonAsync<List<SocialComment>>(_apiUrl);

                if (records != null)
                {
                    _logger.LogInformation("Registros recibidos: {Count}", records.Count);
                    foreach (var record in records)
                    {
                        opinions.Add(new OpinionData
                        {
                            IdOpinion = record.CommentId,
                            IdCliente = record.UserId,
                            IdProducto = record.ProductId,
                            Comentario = record.CommentText,
                            Fuente = "SocialMedia", // Fuente fija para la API
                            Rating = record.Rating, // Ahora sí leemos el Rating
                            Clasificacion = record.Sentiment, // Y también el Sentimiento (Clasificacion)
                            Fecha = !string.IsNullOrEmpty(record.Date) ? DateTime.Parse(record.Date) : default
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error en extracción API");
            }
            return opinions;
        }

        public string GetSourceName() => "API REST - Comentarios Sociales";

        // ESTA ES LA CLASE CORRECTA Y COMPLETA QUE COINCIDE CON TU API
        private class SocialComment
        {
            public string? CommentId { get; set; }
            public string? UserId { get; set; }
            public string? ProductId { get; set; }
            public string? Date { get; set; } // Nombre correcto de la propiedad de fecha
            public string? CommentText { get; set; }
            public int? Rating { get; set; }
            public string? Sentiment { get; set; }
        }
    }
}