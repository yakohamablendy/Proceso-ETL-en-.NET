# Proyecto de Proceso ETL en .NET

Este es el repositorio para la Actividad 1 de la materia Electiva 1.

## ¿De qué se trata?

Este proyecto es un servicio automático (un "Worker Service") hecho en .NET 8. Su trabajo es recolectar opiniones de clientes desde tres lugares diferentes para juntarlas en un solo sitio.

El proceso completo es la primera parte de un ETL, específicamente la fase de **Extracción**.

### Fuentes de Información
El programa extrae datos desde:
1.  Un **archivo CSV** con encuestas internas.
2.  Una **base de datos SQL Server** con reseñas de una página web.
3.  Una **API REST** local que simula comentarios de redes sociales.

Al final, todos estos datos se guardan en una tabla temporal (Staging) para poder analizarlos después.

## Estructura del Proyecto
La solución tiene dos partes:
- **`ETL.OpinionesWorker`:** Este es el servicio principal que hace todo el trabajo de extracción y carga.
- **`CommentsAPI`:** Una API muy simple que creé para simular una de las fuentes de datos.

## ¿Cómo se puede ejecutar?

#### Lo que necesitas:
- .NET 8 SDK.
- SQL Server (con una base de datos y la tabla `Staging.OpinionesStaging` ya creadas).

#### Pasos para probarlo:
1.  Asegúrate de que la base de datos `DWOpiniones` esté lista.
2.  Revisa las cadenas de conexión en el archivo `appsettings.json` del proyecto `ETL.OpinionesWorker` y pon las de tu computadora.
3.  Haz lo mismo con la ruta del archivo `social_comments.csv` en el `appsettings.json` del proyecto `CommentsAPI`.
4.  Desde la terminal, ejecuta primero el proyecto `CommentsAPI` con `dotnet run`.
5.  En otra terminal, ejecuta el proyecto `ETL.OpinionesWorker` con `dotnet run`.
