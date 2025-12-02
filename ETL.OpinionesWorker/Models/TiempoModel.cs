using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETL.OpinionesWorker.Models
{
    public class TiempoModel
    {
        public DateTime Fecha { get; set; }
        public int Año { get; set; }
        public int Mes { get; set; }
        public int Trimestre { get; set; }
    }
}