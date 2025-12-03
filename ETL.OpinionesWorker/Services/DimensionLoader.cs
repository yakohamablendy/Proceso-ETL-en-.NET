using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Globalization;
using System.IO;

namespace ETL.OpinionesWorker.Services
{
    public class DimensionLoader
    {
        private readonly string _connectionString;
        private readonly ILogger<DimensionLoader> _logger;
        
        private const string RUTA_BASE = @"E:\Clases\Universidad\Penultimo Trimestre\Electiva 1\Asignaciónes\Actividad 1 Desarrollo del Proceso ETL en NET(Arquitectura)\Archivo CSV Análisis de Opiniones de Clientes-20251107\";

        public DimensionLoader(IConfiguration configuration, ILogger<DimensionLoader> logger)
        {
            _connectionString = configuration.GetConnectionString("DWOpiniones");
            _logger = logger;
        }

        public async Task CargarTodasLasDimensiones()
        {
            try
            {
                _logger.LogInformation(">>> RE-CARGANDO DIMENSIONES (CON IDs ORIGINALES) <<<");

                await LimpiarDimensiones();
                await CargarClientes();
                await CargarProductos();
                await CargarFuentes();
                await CargarTiempo();

                _logger.LogInformation(">>> DIMENSIONES ACTUALIZADAS CORRECTAMENTE <<<");
            }
            catch (Exception ex)
            {
                _logger.LogError($"ERROR CRÍTICO EN DIMENSIONES: {ex.Message}");
                throw; 
            }
        }

        private async Task LimpiarDimensiones()
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
           
            string sql = @"
                DELETE FROM Fact.FactOpiniones;
                DELETE FROM Dimension.Cliente;
                DELETE FROM Dimension.DimProducto;
                DELETE FROM Dimension.DimFuente;
                DELETE FROM Dimension.DimTiempo;
                -- Reiniciamos contadores (aunque usaremos IDs manuales, es buena práctica)
                DBCC CHECKIDENT ('Dimension.Cliente', RESEED, 0);
                DBCC CHECKIDENT ('Dimension.DimProducto', RESEED, 0);
                DBCC CHECKIDENT ('Dimension.DimFuente', RESEED, 0);
            ";
            using var cmd = new SqlCommand(sql, conn);
            await cmd.ExecuteNonQueryAsync();
        }

        private async Task CargarClientes()
        {
            var ruta = Path.Combine(RUTA_BASE, "clients.csv");
            if (!File.Exists(ruta)) return;

            using var reader = new StreamReader(ruta);
            using var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture));
            var registros = csv.GetRecords<dynamic>();

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

       
            await new SqlCommand("SET IDENTITY_INSERT Dimension.Cliente ON", conn).ExecuteNonQueryAsync();

            foreach (var r in registros)
            {
                string query = "INSERT INTO Dimension.Cliente (IdCliente, NombreCliente, Email, Pais, Segmento) VALUES (@Id, @Nom, @Email, 'Desconocido', 'General')";
                using var cmd = new SqlCommand(query, conn);

             
                int idCliente = int.Parse(r.IdCliente.ToString());

                cmd.Parameters.AddWithValue("@Id", idCliente);
                cmd.Parameters.AddWithValue("@Nom", (string)r.Nombre);
                cmd.Parameters.AddWithValue("@Email", (string)r.Email);
                await cmd.ExecuteNonQueryAsync();
            }

            await new SqlCommand("SET IDENTITY_INSERT Dimension.Cliente OFF", conn).ExecuteNonQueryAsync();
            _logger.LogInformation("Dimension.Cliente cargada.");
        }

        private async Task CargarProductos()
        {
            var ruta = Path.Combine(RUTA_BASE, "products.csv");
            if (!File.Exists(ruta)) return;

            using var reader = new StreamReader(ruta);
            using var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture));
            var registros = csv.GetRecords<dynamic>();

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            await new SqlCommand("SET IDENTITY_INSERT Dimension.DimProducto ON", conn).ExecuteNonQueryAsync();

            foreach (var r in registros)
            {
                string query = "INSERT INTO Dimension.DimProducto (IdProducto, NombreProducto, Categoria) VALUES (@Id, @Nom, @Cat)";
                using var cmd = new SqlCommand(query, conn);

           
                int idProducto = int.Parse(r.IdProducto.ToString());

                cmd.Parameters.AddWithValue("@Id", idProducto);
                cmd.Parameters.AddWithValue("@Nom", (string)r.Nombre);
                cmd.Parameters.AddWithValue("@Cat", (string)r.Categoría);
                await cmd.ExecuteNonQueryAsync();
            }

            await new SqlCommand("SET IDENTITY_INSERT Dimension.DimProducto OFF", conn).ExecuteNonQueryAsync();
            _logger.LogInformation("Dimension.DimProducto cargada.");
        }

        private async Task CargarFuentes()
        {
            var ruta = Path.Combine(RUTA_BASE, "fuente_datos.csv");
            if (!File.Exists(ruta)) return;

            using var reader = new StreamReader(ruta);
            using var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture));
            var registros = csv.GetRecords<dynamic>();

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            foreach (var r in registros)
            {
                string query = "INSERT INTO Dimension.DimFuente (NombreFuente) VALUES (@Nom)";
                using var cmd = new SqlCommand(query, conn);
                cmd.Parameters.AddWithValue("@Nom", (string)r.TipoFuente);
                await cmd.ExecuteNonQueryAsync();
            }

           
            string fixQuery = "IF NOT EXISTS (SELECT 1 FROM Dimension.DimFuente WHERE NombreFuente = 'EncuestaInterna') INSERT INTO Dimension.DimFuente (NombreFuente) VALUES ('EncuestaInterna')";
            await new SqlCommand(fixQuery, conn).ExecuteNonQueryAsync();

            _logger.LogInformation("Dimension.DimFuente cargada.");
        }

        private async Task CargarTiempo()
        {
            DateTime inicio = new DateTime(2024, 09, 01);
            DateTime fin = new DateTime(2025, 12, 31);

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            for (var dia = inicio; dia <= fin; dia = dia.AddDays(1))
            {
                string query = "INSERT INTO Dimension.DimTiempo (FechaCompleta, Año, Mes, Trimestre) VALUES (@F, @A, @M, @T)";
                using var cmd = new SqlCommand(query, conn);
                cmd.Parameters.AddWithValue("@F", dia);
                cmd.Parameters.AddWithValue("@A", dia.Year);
                cmd.Parameters.AddWithValue("@M", dia.Month);
                cmd.Parameters.AddWithValue("@T", (dia.Month - 1) / 3 + 1);
                await cmd.ExecuteNonQueryAsync();
            }
            _logger.LogInformation("Dimension.DimTiempo generada.");
        }
    }
}