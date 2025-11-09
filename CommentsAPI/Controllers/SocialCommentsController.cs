using Microsoft.AspNetCore.Mvc;

namespace CommentsAPI.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class SocialCommentsController : ControllerBase
    {
        private readonly IConfiguration _configuration;

        public SocialCommentsController(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        [HttpGet]
        public IActionResult GetComments()
        {
            try
            {
                var csvPath = _configuration["CsvFilePath"];

                if (!System.IO.File.Exists(csvPath))
                    return NotFound("Archivo CSV no encontrado");

                var lines = System.IO.File.ReadAllLines(csvPath).Skip(1);

                var comments = lines.Select(line =>
                {
                    var values = line.Split(',');
                    return new
                    {
                        IdComment = values[0],
                        IdCliente = values[1],
                        IdProducto = values[2],
                        Fuente = values[3],
                        Fecha = values[4],
                        Comentario = values[5]
                    };
                });

                return Ok(comments);
            }
            catch (Exception ex)
            {
                return StatusCode(500, $"Error: {ex.Message}");
            }
        }
    }
}