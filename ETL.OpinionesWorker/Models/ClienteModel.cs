using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETL.OpinionesWorker.Models
{
    public class ClienteModel
    {
        public int IdCliente { get; set; }
        public string Nombre { get; set; }
        public string Email { get; set; }
        public string Pais { get; set; }
        public string Segmento { get; set; }
    }
}
