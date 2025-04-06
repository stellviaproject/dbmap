package pgutil

import (
	"fmt"
	"os"
	"os/exec"
)

func ExecSQLFiles(database, user, password, host string, port int, sqlFiles ...string) error {
	for _, file := range sqlFiles {
		// Configura el comando psql para ejecutar el archivo .sql
		cmd := exec.Command(
			"psql",
			"-U", user, // Usuario de la base de datos
			"-h", host, // Host de la base de datos
			"-p", fmt.Sprintf("%d", port), // Puerto de la base de datos
			"-d", database, // Base de datos de destino
			"-f", file, // Archivo .sql a ejecutar
		)

		// Pasar la contraseña como variable de entorno
		cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", password))

		// Ejecutar el comando
		fmt.Printf("Ejecutando archivo SQL: %s\n", file)
		if output, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("error ejecutando archivo %s: %v\nSalida: %s", file, err, string(output))
		}
		fmt.Printf("Archivo %s ejecutado correctamente.\n", file)
	}
	return nil
}
