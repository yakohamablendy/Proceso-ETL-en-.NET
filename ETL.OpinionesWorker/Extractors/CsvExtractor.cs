using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration;
using ETL.OpinionesWorker.Models;
using Microsoft.Extensions.Logging;
using CsvHelper.Configuration.Attributes; 

namespace ETL.OpinionesWorker.Extractors
{
    public class CsvExtractor : IExtractor
    {
        private readonly string _filePath;
        private readonly ILogger<CsvExtractor> _logger;

        public CsvExtractor(string filePath, ILogger<CsvExtractor> logger)
        {
            _filePath = filePath;
            _logger = logger;
        }

        public async Task<List<OpinionData>> ExtractAsync()
        {
            var opinions = new List<OpinionData>();

            try
            {
                _logger.LogInformation($"Iniciando extracción CSV: {_filePath}");

                if (!File.Exists(_filePath))
                {
                    _logger.LogError($"Archivo no existe: {_filePath}");
                    return opinions;
                }

                var config = new CsvConfiguration(CultureInfo.InvariantCulture)
                {
                    HasHeaderRecord = true,
                    MissingFieldFound = null,
                    BadDataFound = null,
                    Delimiter = ",",
                    TrimOptions = TrimOptions.Trim
                };

                using (var reader = new StreamReader(_filePath))
                using (var csv = new CsvReader(reader, config))
                {
                    await Task.Run(() =>
                    {
                        var records = csv.GetRecords<SurveyRecord>().ToList();
                        _logger.LogInformation($"Registros leídos: {records.Count}");

                        foreach (var record in records)
                        {
                            opinions.Add(new OpinionData
                            {
                                IdOpinion = record.IdOpinion.ToString(),
                                IdCliente = record.IdCliente.ToString(),
                                IdProducto = record.IdProducto.ToString(),
                                Fecha = DateTime.Parse(record.Fecha),
                                Comentario = record.Comentario,
                                Fuente = record.Fuente,
                                Rating = record.PuntajeSatisfaccion,
                                Clasificacion = record.Clasificacion
                            });
                        }
                    });

                    _logger.LogInformation($"Extracción completada: {opinions.Count} opiniones");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error en extracción CSV");
            }

            return opinions;
        }

        public string GetSourceName() => "CSV - Encuestas Internas";

        private class SurveyRecord
        {
            public int IdOpinion { get; set; }
            public int IdCliente { get; set; }
            public int IdProducto { get; set; }
            public string? Fecha { get; set; }
            public string? Comentario { get; set; }

            [Name("Clasificación")] 
            public string? Clasificacion { get; set; }

            [Name("PuntajeSatisfacción")] 
            public int PuntajeSatisfaccion { get; set; }
            public string? Fuente { get; set; }
        }
    }
}