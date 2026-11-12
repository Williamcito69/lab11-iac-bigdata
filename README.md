# Laboratorio 11 — Infraestructura Big Data con Terraform y Floci

## Objetivo

Aprovisionar una plataforma de datos local mediante Infrastructure as Code,
utilizando Terraform y Floci como emulador de servicios AWS.

## Herramientas

- Docker Desktop con motor Linux.
- Docker Compose.
- Terraform
- AWS CLI v2.
- Git.

## Infraestructura

- Bucket S3: ucv-data-platform.
- Dominios: sales, customers, products y marketing.
- Capas por dominio: bronze, silver y gold.
- Bases de datos Glue: sales_domain, customers_domain,
  products_domain y marketing_domain.
- Endpoint local: http://localhost:4566.

La configuración final administra 17 recursos:
1 bucket, 12 objetos que representan prefijos y 4 bases de datos Glue.

S3 utiliza claves y prefijos; no tiene carpetas reales.
Las bases de datos Glue almacenarán metadatos y todavía no contienen tablas.

## Archivos

- docker-compose.yml: configuración de Floci.
- terraform/versions.tf: versiones requeridas.
- terraform/provider.tf: conexión de Terraform con Floci.
- terraform/variables.tf: parámetros, dominios y capas.
- terraform/main.tf: recursos de S3 y Glue.
- terraform/outputs.tf: resultados del despliegue.

## Ejecución en PowerShell

Desde la carpeta principal, con Docker Desktop funcionando:

```powershell
docker compose up -d
docker compose ps
cd terraform
terraform init
terraform fmt
terraform validate
terraform plan
terraform apply
```

Confirmar con yes cuando Terraform lo solicite.

## Verificación mediante AWS CLI

```powershell
$env:AWS_ACCESS_KEY_ID = "test"
$env:AWS_SECRET_ACCESS_KEY = "test"
$env:AWS_DEFAULT_REGION = "us-east-1"
$env:AWS_PAGER = ""

aws --endpoint-url http://localhost:4566 s3 ls s3://ucv-data-platform/ --recursive
aws --endpoint-url http://localhost:4566 glue get-databases --catalog-id 000000000000
```

## Estado e idempotencia

Desde la carpeta terraform:

```powershell
terraform state list
terraform plan
```

Una configuración sincronizada muestra:
No changes. Your infrastructure matches the configuration.

El estado relaciona el código con los recursos existentes.
No debe editarse manualmente ni publicarse en GitHub.

## Cambio incremental

La configuración inicial contemplaba sales, customers y products:
13 recursos en total.

Se agregó marketing a la variable domains para incorporar tres
prefijos S3 y una base de datos Glue. La configuración final contiene
17 recursos y el plan posterior no propone cambios.

## Eliminación de la infraestructura

Desde la carpeta terraform, con Floci funcionando:

```powershell
terraform destroy
```

Revisar el plan y confirmar con yes.
Este comando elimina los recursos administrados por Terraform.
Se pueden volver a crear mediante terraform apply.

## Alcance

Este laboratorio prepara infraestructura y catálogos.
No realiza ingesta ni transformación de datos.
La organización por dominios introduce conceptos de Data Mesh,
pero no constituye por sí sola una implementación completa.

## Archivos excluidos de Git

Se excluyen data/, .terraform/, archivos de estado y planes guardados.
Se incluye .terraform.lock.hcl para conservar la selección del provider.