using Microsoft.AspNetCore.Mvc;

namespace API.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class SocialCommentsController : ControllerBase
    {
        private readonly string _csvFilePath;
        private readonly ILogger<SocialCommentsController> _logger;

        public SocialCommentsController(IConfiguration configuration, ILogger<SocialCommentsController> logger)
        {
            _csvFilePath = configuration["CsvFilePath"] ?? "";
            _logger = logger;
        }

        [HttpGet]
        public IActionResult GetComments()
        {
            try
            {
                if (!System.IO.File.Exists(_csvFilePath))
                {
                    return NotFound("Archivo CSV no encontrado");
                }

                var comments = new List<SocialComment>();
                var lines = System.IO.File.ReadAllLines(_csvFilePath).Skip(1); // Skip header

                foreach (var line in lines)
                {
                    var values = line.Split(',');

                    comments.Add(new SocialComment
                    {
                        IdComment = values[0],
                        IdCliente = values[1],
                        IdProducto = values[2],
                        Fuente = values[3],
                        Fecha = values[4],
                        Comentario = values[5].Trim('"')
                    });
                }

                _logger.LogInformation($"Retornando {comments.Count} comentarios");
                return Ok(comments);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al leer CSV");
                return StatusCode(500, "Error interno del servidor");
            }
        }
    }

    public class SocialComment
    {
        public string IdComment { get; set; }
        public string IdCliente { get; set; }
        public string IdProducto { get; set; }
        public string Fuente { get; set; }
        public string Fecha { get; set; }
        public string Comentario { get; set; }
    }
}