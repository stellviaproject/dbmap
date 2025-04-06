package pgutil

import (
	"fmt"
	"os"
	"os/exec"
)

func GetDatabaseDefinition(database, user, password, host string, port int, outputFile string) (string, error) {
	// Configura el comando pg_dump
	cmd := exec.Command(
		"pg_dump",
		"-U", user, // Usuario de la base de datos
		"-h", host, // Host de la base de datos
		"-p", fmt.Sprintf("%d", port),
		"-d", database, // Base de datos de origen
		"--schema-only",      // Exportar solo el esquema (estructura)
		"--no-owner",         // Excluir el propietario
		"--no-acl",           // Excluir permisos (ACL)
		"--no-comments",      // Excluir comentarios
		"--disable-triggers", // Excluir triggers
		"-f", outputFile,     // Archivo de salida
	)

	// Pasar la contraseña a pg_dump usando la variable de entorno
	cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", password))

	// Ejecutar el comando
	fmt.Printf("Generando archivo SQL para la base de datos %s en %s\n", database, outputFile)
	if output, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("error generando definición de base de datos: %v\nSalida: %s", err, string(output))
	}

	fmt.Printf("Archivo SQL generado correctamente: %s\n", outputFile)
	return outputFile, nil
}
